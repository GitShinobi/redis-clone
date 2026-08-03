package main

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"os"
	"redis/resp"
	"strconv"
	"strings"
	"sync"
	"time"
	"slices"
)
type entry struct {
    kind    string
    strVal  *string
    listVal *[]string
    hashVal *map[string]string
    setVal *map[string]struct{}
    expire  time.Time
}

var shards [NumShards]map[string]entry
var shardLocks [NumShards]sync.RWMutex
var cmdBuffer [][]string
var cmdBufMu sync.Mutex
var fileMu sync.RWMutex

const (
    NumShards = 16
    RewriteInterval = 24 * time.Hour
    FlushThreshold = 10
)

func init() {
	for i := 0; i < NumShards; i++ {
		shards[i] = make(map[string]entry)
	}
}
func main() {
	listener, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		fmt.Println(err)
	}
	defer listener.Close()
	nowFormatted := time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00")
	fmt.Printf("1:C %s # oO0OoO0OoO0Oo Redis is starting oO0OoO0OoO0Oo\n", nowFormatted)
	fileMu.RLock()
	aof, err := os.ReadFile("aof.log")
	fileMu.RUnlock()
	if err == nil {
		fmt.Printf("1:C %s # Configuration loaded\n",nowFormatted)
		byteReader := bytes.NewReader(aof)
		reader := bufio.NewReader(byteReader)
		for {
			cmds, err := readCommand(reader)
			if err != nil {
				if err == io.EOF {
					break
				} else {
					fmt.Println("failed aof recovering : ", err)
					return
				}
			}
			writeResponse(cmds, true)
		}
	}
	go func() {
		for {
			rewriteAOF()
			time.Sleep(RewriteInterval)
		}
	}()
	go func() {
		for {
			for i := 0; i < NumShards; i++ {
				keys := []string{}
				shardLocks[i].Lock()
				for k := range shards[i] {
					keys = append(keys, k)
				}
				for _, k := range keys {
					removeIfExpired(k, i)
				}
				shardLocks[i].Unlock()
			}
			cmdBufMu.Lock()
			if len(cmdBuffer) > FlushThreshold {
				appendCommand(cmdBuffer, "aof.log")
				cmdBuffer = [][]string{}
			}
			cmdBufMu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		cmds, err := readCommand(reader)
		if err != nil {
			if err == io.EOF {
				fmt.Println("client disconnected : ", err)
			} else {
				fmt.Println("failed reading command : ", err)
			}
			return
		}
		if len(cmds) == 0 {
			fmt.Println("no command found")
			return
		}
		response := writeResponse(cmds, false)
		conn.Write(response)
	}
}

func readCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	nbrCmd, err := strconv.Atoi(GetCmdSize(line))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	var cmds []string
	for i := 0; i < nbrCmd; i++ {
		size, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		sizeVal, err := strconv.Atoi(GetCmdSize(size))
		if err != nil {
			fmt.Println("failed conversion : ", err)
			return nil, err
		}
		cmdBuf := make([]byte, sizeVal)
		_, err = io.ReadFull(reader, cmdBuf)
		if err != nil {
			return nil, err
		}

		cmdVal := string(cmdBuf)
		cmds = append(cmds, cmdVal)

		_, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
	}
	return cmds, nil
}

func GetCmdSize(chunk string) string {
	strTab := strings.Split(chunk, "\r\n")
	valStr := strTab[0]
	return valStr[1:]
}

func writeResponse(cmds []string, isReplay bool) []byte {
	response := []byte{}
	var index int
	cmdsLen := len(cmds)
	if cmdsLen > 1 {
		index = shardIndex(cmds[1], NumShards)
	}
	switch strings.ToUpper(cmds[0]) {
	case "PING":
		return resp.Ping()
	case "SET":
		if err := resp.ErrorMsg(cmdsLen, 3, cmds[0], "=="); err != nil {
    		return err
		}
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		val := cmds[2]
		shards[index][key] = entry{"string",&val,nil,nil,nil,time.Time{}}
		logToAOF(cmds,isReplay)
		return resp.OkResponse()
	case "GET":
		if err := resp.ErrorMsg(cmdsLen, 2, cmds[0], "=="); err != nil {
    		return err
		}
		shardLocks[index].RLock()
		defer shardLocks[index].RUnlock()
		key := cmds[1]
		data, ok := shards[index][key]
		if !ok || isExpired(key, index) {
			return resp.NullResponse()
		}
		return resp.BulkResponse(*data.strVal)
	case "DEL":
		if err := resp.ErrorMsg(cmdsLen, 2, cmds[0], "=="); err != nil {
    		return err
		}
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		removeIfExpired(key, index)
		_, ok := shards[index][key]
		if ok {
			logToAOF(cmds,isReplay)
			delete(shards[index], key)
			return resp.IntResponse(1)
		} else {
			return resp.IntResponse(0)
		}
 	case "EXISTS":
		if err := resp.ErrorMsg(cmdsLen, 2, cmds[0], "=="); err != nil {
    		return err
		}
		shardLocks[index].RLock()
		defer shardLocks[index].RUnlock()
		key := cmds[1]
		if isExpired(key, index){return resp.IntResponse(0)}
		_, ok := shards[index][key]
		if ok {
			return resp.IntResponse(1)
		} else {
			return resp.IntResponse(0)
		}
 	case "EXPIRE":
		if err := resp.ErrorMsg(cmdsLen, 3, cmds[0], "=="); err != nil {
    		return err
		}
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		data, ok := shards[index][key]
		if ok {
			removeIfExpired(key, index)
			exp, err := strconv.Atoi(cmds[2])
			if err != nil {
				fmt.Println(err)
				return resp.ErrorResponse("value is not an integer")
			} else {
				duration := time.Duration(exp) * time.Second
				data.expire = time.Now().Add(duration)
				cmdBufMu.Lock()
				if !isReplay {
					cmdBuffer = append(cmdBuffer, cmds)
				}
				cmdBufMu.Unlock()
				shards[index][key] = data
				return resp.IntResponse(1)
			}
		} else {
			return resp.IntResponse(0)
		}
 	case "TTL":
		if err := resp.ErrorMsg(cmdsLen, 2, cmds[0], "=="); err != nil {
    		return err
		}
		shardLocks[index].RLock()
		defer shardLocks[index].RUnlock()
		key := cmds[1]
		if isExpired(key, index){return resp.IntResponse(-2)}
		data, ok := shards[index][key]
		if ok {
			if data.expire.IsZero() {
				return resp.IntResponse(-1)
			} else {
				return resp.IntResponse(int(data.expire.Sub(time.Now()).Seconds()))
			}
		} else {
				return resp.IntResponse(-2)
		}
 	case "EXPIREAT":
		if err := resp.ErrorMsg(cmdsLen, 3, cmds[0], "=="); err != nil {
    		return err
		}
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		removeIfExpired(key, index)
		data, ok := shards[index][key]
		if ok {
			exp, err := strconv.Atoi(cmds[2])
			if err != nil {
				fmt.Println(err)
				return resp.ErrorResponse("value is not an integer")
			} else {
				data.expire = time.Unix(int64(exp), 0)
				shards[index][key] = data
				logToAOF(cmds,isReplay)
				return resp.IntResponse(1)
			}
		} else {
			return resp.IntResponse(0)
		}
 	case "DBSIZE" : 
		if err := resp.ErrorMsg(cmdsLen, 1, cmds[0], "=="); err != nil {
    		return err
		}
		keys := getDBSize() 
		return resp.IntResponse(keys)
 	case "KEYS" : 
	if err := resp.ErrorMsg(cmdsLen, 2, cmds[0], "=="); err != nil {
    		return err
		}
	var keys []string
	for i := 0; i < NumShards; i++ {
		shardLocks[i].RLock()			
		for k := range shards[i] {
        if !isExpired(k, i) {
            keys = append(keys, k)
        }
    		}
		shardLocks[i].RUnlock()	
	}
	if len(keys) == 0{
			return resp.EmptyArrayResponse()
	}
	return buildArrayResponse(keys)
		case "FLUSHALL" : 
		if err := resp.ErrorMsg(cmdsLen, 1, cmds[0], "=="); err != nil {
    		return err
		}
		for i := 0; i < NumShards; i++ {
			shardLocks[i].Lock()
				clear(shards[i])
			shardLocks[i].Unlock()
		}
		return resp.OkResponse()
	case "LPUSH", "RPUSH" : 
		if err := resp.ErrorMsg(cmdsLen, 3,cmds[0], ">="); err != nil {
    		return err
		}
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		removeIfExpired(key, index)
		data, ok := shards[index][key]
		var newValues []string
		for i:=2; i<cmdsLen; i++{
			newValues = append(newValues,cmds[i])
				}
		if ok {
				if data.kind != "list"{
				return resp.ErrorResponse("value not a list")}
				if strings.ToUpper(cmds[0])[0] == 'L'{
						slices.Reverse(newValues)   
						*(data.listVal) = append(newValues,*data.listVal...)
				}else {
						*(data.listVal) = append(*data.listVal,newValues...)}
		} else {
			if strings.ToUpper(cmds[0])[0] == 'L'{
				slices.Reverse(newValues)
			}
			data = entry{"list",nil,&newValues,nil,nil, time.Time{}}
			shards[index][key] = data
		}
		shards[index][key] = data
		logToAOF(cmds,isReplay)
		return resp.IntResponse(len(*(data.listVal)))
	case "LRANGE" : 
		if err := resp.ErrorMsg(cmdsLen, 4, cmds[0], "=="); err != nil {
    		return err
		}
		key := cmds[1]
		start,startErr := strconv.Atoi(cmds[2])
		stop,stopErr := strconv.Atoi(cmds[3])
		if startErr != nil || stopErr != nil {
			return resp.ErrorResponse("value start/stop is not integer")
		}
		shardLocks[index].RLock()
		defer shardLocks[index].RUnlock()
		if isExpired(key, index){
			return resp.EmptyArrayResponse()
		}
		data, ok := shards[index][key]
		if !ok{
			return  resp.EmptyArrayResponse()
		}
		if data.kind != "list"{
				return resp.ErrorResponse("value not a list")}
		size :=  len(*(data.listVal))
		if size == 0{
				return resp.EmptyArrayResponse()
		}
		if start < 0 {
			 start = size + start 
			 if start < 0 { start = 0 }
							}
		if stop < 0 { stop = size + stop }
		if stop >= size {stop = size-1}
		if start >= size || start > stop  {return resp.EmptyArrayResponse()}
		return buildArrayResponse((*data.listVal)[start:stop+1])
 	case "LLEN" : 
		if err := resp.ErrorMsg(cmdsLen, 2, cmds[0], "=="); err != nil {
    		return err
		}
		shardLocks[index].RLock()
		defer shardLocks[index].RUnlock()
		key := cmds[1]
		if isExpired(key, index){
			return resp.IntResponse(0)
		}
		data, ok := shards[index][key]
		if !ok || data.kind != "list"{
			return resp.IntResponse(0)
		}
		return resp.IntResponse(len(*(data.listVal)))
 	case "LPOP", "RPOP" : 
		if err := resp.ErrorMsg(cmdsLen, 2, cmds[0], ">="); err != nil {
    		return err
		}
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		countProvided := false
		var count int
		if len(cmds) > 2 {
			nbr, err := strconv.Atoi(cmds[2])
			if err != nil {
			fmt.Println(err)
			return resp.NullResponse()
			}
			count = nbr
			countProvided = true
		}
		removeIfExpired(key, index)
		var removedVals []string
		data, ok := shards[index][key]
		if ok {
				if data.kind != "list"{
				return resp.ErrorResponse("value not a list")}
				size :=  len(*(data.listVal))
				if count < 0 {
				return resp.ErrorResponse("value is not an integer or out of range")}
				if size == 0 {
					delete(shards[index], key)
					if countProvided {
						return resp.EmptyArrayResponse()
					}
					return resp.NullResponse()
				}
				if !countProvided {
					count = 1
				}
				if count > size  {
					count = size
				}
				if strings.ToUpper(cmds[0])[0] == 'L'{
					removedVals = (*data.listVal)[:count]
					(*data.listVal) = (*data.listVal)[count:]
				}else {
					removedVals = (*data.listVal)[len(*data.listVal)-count:]
					(*data.listVal) = (*data.listVal)[:len(*data.listVal)-count]
				}
				logToAOF(cmds,isReplay)
				if !countProvided {
    				return resp.BulkResponse(removedVals[0])
					}
				return buildArrayResponse(removedVals)
		} else {
			if countProvided {
        	return resp.EmptyArrayResponse()
    		}
   		 	return resp.NullResponse()
		}
	case "LPUSHX", "RPUSHX" : 
		if err := resp.ErrorMsg(cmdsLen, 3, cmds[0], ">="); err != nil {
    		return err
		}
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		removeIfExpired(key, index)
		data, ok := shards[index][key]
		var newValues []string
		for i:=2; i<cmdsLen; i++{
			newValues = append(newValues, cmds[i])
				}
		if ok {
				if data.kind != "list"{
				return resp.ErrorResponse("value not a list")}
				if strings.ToUpper(cmds[0])[0] == 'L'{
						slices.Reverse(newValues)   
						*(data.listVal) = append(newValues,*data.listVal...)
				}else {
						*(data.listVal) = append(*data.listVal,newValues...)}
				shards[index][key] = data
				logToAOF(cmds,isReplay)
				return resp.IntResponse(len(*data.listVal))
		} else {
			return resp.IntResponse(0)
		}
	case "SADD":
		if err := resp.ErrorMsg(cmdsLen, 3, cmds[0], ">="); err != nil {
    		return err
		}
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		removeIfExpired(key, index)
		data, ok := shards[index][key]
		if !ok {
			setVal := make(map[string]struct{})
			data = entry{"set",nil,nil,nil,&setVal,time.Time{}}
		}
		if data.kind != "set"{
			return resp.ErrorResponse("value not a set")
			}
		setlen := 0
		for i:=2; i<cmdsLen;i++{
		if _, ok := (*data.setVal)[cmds[i]]; !ok{
			(*data.setVal)[cmds[i]] = struct{}{}
			setlen++
			}
		}
		shards[index][key] = data
		logToAOF(cmds,isReplay)
		return resp.IntResponse(setlen)
	case "SMEMBERS" : 
		if err := resp.ErrorMsg(cmdsLen, 2, cmds[0], "=="); err != nil {
    		return err
		}
		key := cmds[1]
		shardLocks[index].RLock()			
		defer shardLocks[index].RUnlock()
		data, ok := shards[index][key]
		if !ok{
			return resp.EmptyArrayResponse()
		}
		if data.kind != "set"{
				return resp.ErrorResponse("value not a set")
				}	
		if isExpired(key, index) || len(*(data.setVal)) == 0 {
				return resp.EmptyArrayResponse()
		}
		keys := make([]string, 0, len(*data.setVal))
		for k := range *data.setVal {
			keys = append(keys, k)
		}
		return buildArrayResponse(keys)
 	case "SISMEMBER":
		if err := resp.ErrorMsg(cmdsLen, 3, cmds[0], "=="); err != nil {
    		return err
		}
		key := cmds[1]
		member := cmds[2]
		shardLocks[index].RLock()
		defer shardLocks[index].RUnlock()
		if isExpired(key, index){ return resp.IntResponse(0)}	
		data , ok := shards[index][key]
		if !ok {
			 return resp.IntResponse(0)
		}
		if data.kind != "set"{
			return resp.ErrorResponse("value not a set")
			}
		_ , exist := (*data.setVal)[member]
		if exist {
			return resp.IntResponse(1)
		}else{
			return resp.IntResponse(0)
		}
 	case "SCARD" : 
		if err := resp.ErrorMsg(cmdsLen, 2, cmds[0], "=="); err != nil {
    		return err
		}
		key := cmds[1]
		shardLocks[index].RLock()			
		defer shardLocks[index].RUnlock()
		data, ok := shards[index][key]
		if !ok{
			return resp.IntResponse(0)
		}
		if data.kind != "set"{
				return resp.ErrorResponse("value not a set")
				}	
		if isExpired(key, index) || len(*(data.setVal)) == 0 {
				return resp.IntResponse(0)
		}
		return resp.IntResponse(len(*data.setVal))
 	case "SREM" : 
		if err := resp.ErrorMsg(cmdsLen, 3, cmds[0], ">="); err != nil {
    		return err
		}
		key := cmds[1]
		shardLocks[index].Lock()			
		defer shardLocks[index].Unlock()
		if removeIfExpired(key, index) {
				return  resp.IntResponse(0)
		}
		data, ok := shards[index][key]
		if !ok{
			return  resp.IntResponse(0)
		}
		if data.kind != "set"{
				return resp.ErrorResponse("value not a set")
				}	
		removedNbr := 0
		for i:=2; i<cmdsLen; i++{
			_, ok := (*data.setVal)[cmds[i]]
			if ok {
				delete(*data.setVal, cmds[i])
				removedNbr++
			}
		}
		if len(*(data.setVal)) == 0 {
    		delete(shards[index],key)
		}else{
			shards[index][key] = data
		}
		logToAOF(cmds,isReplay)
		return resp.IntResponse(removedNbr)
	case "HSET": 
		if err := resp.ErrorMsg(cmdsLen, 4, cmds[0], "=="); err != nil {
    		return err
		}
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		field := cmds[2]
		val := cmds[3]
		removeIfExpired(key, index)
		data, ok := shards[index][key]
		if ok {
			if data.kind != "hash"{
				return resp.ErrorResponse("value not a hash")
				}
			_, ok := (*data.hashVal)[field]
			if ok {
				return resp.IntResponse(0)
			}else{
				return resp.IntResponse(1)
			}
			(*data.hashVal)[field] = val
			shards[index][key] = data
		}else{
			hashVal := make(map[string]string)
			hashVal[field] = val
			shards[index][key] = entry{"hash",nil,nil,&hashVal,nil,time.Time{}}
			response = resp.IntResponse(1)
		}
		logToAOF(cmds,isReplay)
		return response
	case "HGET" :
		if err := resp.ErrorMsg(cmdsLen, 3, cmds[0], "=="); err != nil {
    		return err
		}
		shardLocks[index].RLock()
		defer shardLocks[index].RUnlock()
		key := cmds[1]
		field := cmds[2]
		data, ok := shards[index][key]
		if !ok || isExpired(key, index) {
			return resp.NullResponse()
		}
		if data.kind != "hash"{
				return resp.ErrorResponse("value not a hash")
				}
		if _, exists := (*data.hashVal)[field]; !exists {
			return resp.NullResponse()
		}
		return resp.BulkResponse((*data.hashVal)[field])
	case "HGETALL" :
		if err := resp.ErrorMsg(cmdsLen, 2, cmds[0], "=="); err != nil {
    		return err
		}
		key := cmds[1]
		shardLocks[index].RLock()			
		defer shardLocks[index].RUnlock()
		data, ok := shards[index][key]
		if !ok{
			return resp.EmptyArrayResponse()
		}
		if data.kind != "hash"{
				return resp.ErrorResponse("value not a hash")
				}	
		if isExpired(key, index) || len(*(data.hashVal)) == 0 {
				return resp.EmptyArrayResponse()
		}
		map_list := []string{}
		for k,v := range *data.hashVal {
			map_list = append(map_list, k)
			map_list = append(map_list, v)
		}
		return buildArrayResponse(map_list)
	case "HDEL": 
		if err := resp.ErrorMsg(cmdsLen, 3, cmds[0], ">="); err != nil {
    		return err
		}
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		removeIfExpired(key, index)
		count := 0
		data, ok := shards[index][key]
		if !ok{
			return  resp.IntResponse(0)
		}
		if data.kind != "hash"{
		return resp.ErrorResponse("value not a hash")}
		for i:= 2; i<cmdsLen; i++{
			_, ok := (*data.hashVal)[cmds[i]]
			if ok {
				delete(*data.hashVal, cmds[i])
				count++
			}
		}
		if len(*(data.hashVal)) == 0 {
    		delete(shards[index],key)
		}else{
			shards[index][key] = data
		}
		logToAOF(cmds, isReplay)
		return resp.IntResponse(count)
	case "HEXISTS":
		if err := resp.ErrorMsg(cmdsLen, 3, cmds[0], "=="); err != nil {
    		return err
		}
		key := cmds[1]
		field := cmds[2]
		shardLocks[index].RLock()
		defer shardLocks[index].RUnlock()
		if isExpired(key, index){return resp.IntResponse(0)}	
		data , ok := shards[index][key]
		if !ok {
			 return resp.IntResponse(0)
		}
		if data.kind != "hash"{
			return resp.ErrorResponse("value not a hash")
			}
		_ , exist := (*data.hashVal)[field]
		if exist {
			return resp.IntResponse(1)
		}else{
			return resp.IntResponse(0)
		}
	case "HLEN" : 
		if err := resp.ErrorMsg(cmdsLen, 2, cmds[0], "=="); err != nil {
    		return err
		}
		key := cmds[1]
		shardLocks[index].RLock()			
		defer shardLocks[index].RUnlock()
		data, ok := shards[index][key]
		if !ok{
			return resp.IntResponse(0)
		}
		if data.kind != "hash"{
				return resp.ErrorResponse("value not a hash")
				}	
		if isExpired(key, index) || len(*(data.hashVal)) == 0 {
				return resp.IntResponse(0)
		}
		return resp.IntResponse(len(*data.hashVal))
 	default:
			return resp.ErrorResponse("unknown command")
	}
}
func removeIfExpired(key string, index int)(bool) {
	if isExpired(key,index) {
		delete(shards[index], key)
		return true
	}
	return false
}

func isExpired(key string, index int) bool {
    data, ok := shards[index][key]
    if !ok {
        return false
    }
    return !data.expire.IsZero() && data.expire.Before(time.Now())
}

func rewriteAOF() {
	var content []byte
	for i := 0; i < NumShards; i++ {
		shardLocks[i].Lock()
		snapshot := make(map[string]entry, len(shards[i]))
		for k, v := range shards[i] {
			snapshot[k] = v
		}
		shardLocks[i].Unlock()
		var timeCmd string
		for key, entry := range snapshot {
			var sb strings.Builder
			if !entry.expire.IsZero() && entry.expire.Before(time.Now()) {
				continue
			}
			switch entry.kind {
			case "string" :
				if entry.strVal == nil {
					continue
				 }
				sb.WriteString(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(*(entry.strVal)), *(entry.strVal)))

			case "list":
				if entry.listVal == nil || len(*entry.listVal) == 0 {
					continue
				}

				sb.WriteString(fmt.Sprintf("*%d\r\n", len(*(entry.listVal))+2)) 
				sb.WriteString("$5\r\nRPUSH\r\n")
				sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(key), key))				
				for i := range *entry.listVal {
					sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len((*entry.listVal)[i]), (*entry.listVal)[i]))
				}
			case "set":
				if entry.setVal == nil || len(*entry.setVal) == 0 {
				continue
					}
				sb.WriteString(fmt.Sprintf("*%d\r\n", len(*(entry.setVal))+2)) 
				sb.WriteString("$4\r\nSADD\r\n")
				sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(key), key))				
				for k := range *entry.setVal {
					sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
				}
			case "hash":
				if entry.hashVal == nil || len(*entry.hashVal) == 0 {
					continue
			}
				sb.WriteString(fmt.Sprintf("*%d\r\n", len(*(entry.hashVal))*2 + 2)) 
				sb.WriteString("$4\r\nHSET\r\n")
				sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(key), key))				
				for k,v := range *entry.hashVal {
					sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
					sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
				}
			default:
				continue
    		}
			valCmd := sb.String()
			content = append(content, []byte(valCmd)...)
			if !entry.expire.IsZero() {
				timeCmd = fmt.Sprintf("*3\r\n$8\r\nEXPIREAT\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(strconv.Itoa(int(entry.expire.Unix()))), strconv.Itoa(int(entry.expire.Unix())))
				content = append(content, []byte(timeCmd)...)
				}
			}		
	}
	fileMu.Lock()
	err := os.WriteFile("aof.log", content, 0666)
	fileMu.Unlock()
	if err != nil {
		fmt.Println(err)
	}
}

func appendCommand(cmdBuffer [][]string, name string) {
	fileMu.Lock()
	defer fileMu.Unlock()
	f, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	for i := range cmdBuffer {
		cmds := resp.EncodeCommand(cmdBuffer[i])
		if _, err := f.Write(cmds); err != nil {
			f.Close()
			fmt.Println(err)
			return
		}
	}

	if err := f.Close(); err != nil {
		fmt.Println(err)
		return
	}
}
func shardIndex(key string, numShards int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % numShards
}

func getDBSize() int{
	keys := 0
	for i := 0; i < NumShards; i++ {
		shardLocks[i].RLock()
		for k, _ := range shards[i] {
           	if isExpired(k,i){
				continue
			}
				keys++
		 }
		shardLocks[i].RUnlock()
	} 
	return keys
}

func logToAOF(cmds []string, isReplay bool) {
    if isReplay {
        return
    }
    cmdBufMu.Lock()
    cmdBuffer = append(cmdBuffer, cmds)
    cmdBufMu.Unlock()
}
func buildArrayResponse(elements []string) []byte {
    response := resp.ArrayResponse(len(elements))
    for _, e := range elements {
        response = append(response, resp.BulkResponse(e)...)
    }
    return response
}