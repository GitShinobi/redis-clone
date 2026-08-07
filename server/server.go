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
var keyVersionsMu sync.Mutex

var keyVersions map[string]int


const (
    NumShards = 16
    RewriteInterval = 24 * time.Hour
    FlushThreshold = 10
)

func init() {
	for i := 0; i < NumShards; i++ {
		shards[i] = make(map[string]entry)
	}
	keyVersions = make(map[string]int)

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
		go HandleConnection(conn)
	}
}

func HandleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	inTransaction := false
	var queue [][]string
	watchedKeys := make(map[string]int)
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
		var response []byte
		if len(cmds) == 0 {
			response =  resp.ErrorResponse("no command found")
			conn.Write(response)
				continue
		}
		min, op := getMinArgs(cmds[0]) 
		if err := resp.ErrorMsg(len(cmds), min, cmds[0], op); err != nil {
				conn.Write(err)
				continue
			}
		index := 0
		if len(cmds) > 1 {
			index = shardIndex(cmds[1], NumShards)
		}
		switch strings.ToUpper(cmds[0]) {
			case "MULTI" :
				if inTransaction {
					response = resp.ErrorResponse("MULTI calls can not be nested")
					conn.Write(response)
					continue	
				}
				inTransaction = true
				queue = [][]string{}
				conn.Write(response)
			case "WATCH" :
				shardLocks[index].Lock()
				for i := 1; i < len(cmds); i++ { 
					key := cmds[i]
					keyVersionsMu.Lock()
					watchedKeys[key] = 	keyVersions[key]
					keyVersionsMu.Unlock()
				}
				shardLocks[index].Unlock()
				response = resp.OkResponse()
				conn.Write(response)
			case "UNWATCH" :
				clear(watchedKeys)
				response = resp.OkResponse()
				conn.Write(response)
				continue
 			case "EXEC" :
				if !inTransaction {
					response = resp.ErrorResponse("EXEC without MULTI")
					conn.Write(response)
					continue	
					}
				abort := false
				keyVersionsMu.Lock()
				for key := range watchedKeys {
					if watchedKeys[key] != keyVersions[key]{
						abort = true
						break
					}
				}
				keyVersionsMu.Unlock()
				clear(watchedKeys)
				if abort {
					inTransaction = false
					queue = nil
					response = resp.NullResponse()
					conn.Write(response)
					continue}
				arrayLen := fmt.Sprintf("*%d\r\n", len(queue))
				response = append(response, []byte(arrayLen)...)
				for i := range queue {
					response = append(response, writeResponse((queue)[i], false)...)
				}
				conn.Write(response)
				inTransaction = false
				queue = nil
			case "DISCARD" :
				if !inTransaction {
					response = resp.ErrorResponse("DISCARD without MULTI")
					conn.Write(response)
					continue
					}
				inTransaction = false
				clear(watchedKeys)
				queue = nil
				response = resp.OkResponse()
				conn.Write(response)
			default :
			if inTransaction {
				queue = append(queue,cmds)
				response = resp.QueuedResponse()
				conn.Write(response)
			}else{
				response = writeResponse(cmds, false)
				conn.Write(response)
			}
		}
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
	min, op := getMinArgs(cmds[0]) 
	if err := resp.ErrorMsg(len(cmds), min, cmds[0], op); err != nil {
    		return err
		}
	index := 0
	if len(cmds) > 1 {
		index = shardIndex(cmds[1], NumShards)
	}
	switch strings.ToUpper(cmds[0]) {
	case "PING":
		return HandlePing()
	case "SET":
		return HandleSet(cmds, index, isReplay)
	case "GET":
		return HandleGet(cmds, index, isReplay)
	case "DEL":
		return HandleDel(cmds, index, isReplay)
 	case "EXISTS":
		return HandleExists(cmds, index, isReplay)
 	case "EXPIRE":
		return HandleExpire(cmds, index, isReplay)
 	case "TTL":
		return HandleTTL(cmds, index, isReplay)
 	case "EXPIREAT":
		return HandleExpireAt(cmds, index, isReplay)
 	case "DBSIZE" : 
		return HandleDBSize()
	case "KEYS" :
		return HandleKeys(cmds, index, isReplay)
	case "FLUSHALL" : 
		return HandleFlushAll()
	case "LPUSH": 
		return HandleLPush(cmds, index, isReplay)
	case "RPUSH" : 
		return HandleRPush(cmds, index, isReplay)
	case "LRANGE" : 
		return HandleLRange(cmds, index, isReplay)
 	case "LLEN" : 
		return HandleLLen(cmds, index, isReplay)
 	case "LPOP" : 
		return HandleLPop(cmds, index, isReplay)
 	case "RPOP" : 
		return HandleRPop(cmds, index, isReplay)
	case "LPUSHX" : 
		return HandleLPushX(cmds, index, isReplay)
	case "RPUSHX" :
		return HandleRPushX(cmds, index, isReplay)
	case "SADD":
		return HandleSAdd(cmds, index, isReplay)
	case "SMEMBERS" : 
		return HandleSMembers(cmds, index, isReplay)
 	case "SISMEMBER":
		return HandleSIsMember(cmds, index, isReplay)
 	case "SCARD" : 
		return HandleSCard(cmds, index, isReplay)
 	case "SREM" : 
		return HandleSRem(cmds, index, isReplay)
	case "HSET": 
		return HandleHSet(cmds, index, isReplay)
	case "HGET" :
		return HandleHGet(cmds, index, isReplay)
	case "HGETALL" :
		return HandleHGetAll(cmds, index, isReplay)
	case "HDEL": 
		return HandleHDel(cmds, index, isReplay)
	case "HEXISTS":
		return HandleHExists(cmds, index, isReplay)
	case "HLEN" : 
		return HandleHLen(cmds, index, isReplay)
	case "LINDEX":
		return HandleLIndex(cmds, index, isReplay)
	case "LSET" :
		return HandleLSet(cmds, index, isReplay)
	case "LTRIM" :
		return HandleLTrim(cmds, index, isReplay)
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
		for k := range shards[i] {
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


func getMinArgs(cmd string) (int,string){
	op := "=="
	min :=1
	switch strings.ToUpper(cmd) {
		case "SET":
			min = 3
		case "GET":
			min = 2
		case "DEL":
			min = 2
			op = ">="
		case "EXISTS":
			min = 2
			op = ">="
		case "EXPIRE":
			min = 3
		case "TTL":
			min = 2 
		case "EXPIREAT":
			min = 3
		case "KEYS" : 
			min = 2
		case "LPUSH", "RPUSH" : 
			min = 3
		case "LRANGE" : 
			min = 4
		case "LLEN" : 
			min = 2
		case "LPOP", "RPOP" : 
			min = 2
			op = ">="
		case "LPUSHX", "RPUSHX" :  
			min = 3
			op = ">="
		case "SADD":
			min = 3
			op = ">="
		case "SMEMBERS" : 
			min = 2
		case "SISMEMBER":
			min = 3
		case "SCARD" : 
			min = 2
		case "SREM" : 
			min = 3
			op = ">="
		case "HSET": 
			min = 4
		case "HGET" :
			min = 3
		case "HGETALL" :
			min = 2
		case "HDEL": 
			min = 3
			op = ">="
		case "HEXISTS":
			min = 3
		case "HLEN" : 
			min = 2
		case "LINDEX":
			min = 3
		case "LSET":
			min = 4
		case "LTRIM":
			min = 4
		case "PING", "DBSIZE", "FLUSHALL", "MULTI", "EXEC", "DISCARD":
    		min = 1
	}
	return min, op
}


func HandlePing() []byte {
    return resp.Ping()
}
	func HandleSet(cmds []string, index int, isReplay bool) []byte {
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		val := cmds[2]
		incKeyVersion(key)
		shards[index][key] = entry{"string",&val,nil,nil,nil,time.Time{}}
		logToAOF(cmds,isReplay)
		return resp.OkResponse()
	}
		
	func HandleGet(cmds []string, index int, isReplay bool) []byte {
		shardLocks[index].RLock()
		defer shardLocks[index].RUnlock()
		key := cmds[1]
		data, ok := shards[index][key]
		if !ok || isExpired(key, index) {
			return resp.NullResponse()
		}
		return resp.BulkResponse(*data.strVal)
	}
	func HandleDel(cmds []string, index int, isReplay bool) []byte {
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		removedNbr := 0
		for i := 1; i < len(cmds); i++ { 
			key := cmds[i]
        	removeIfExpired(key, index)
			_, ok := shards[index][key]
			if ok {
				incKeyVersion(key)
				delete(shards[index], key)
				removedNbr++	
			}
		}
		if removedNbr > 0 {
    		logToAOF(cmds, isReplay)
			}
		return resp.IntResponse(removedNbr)
	}
 	func HandleExists(cmds []string, index int, isReplay bool) []byte {
		shardLocks[index].RLock()
		defer shardLocks[index].RUnlock()
		nbrExist := 0
		for i := 1; i < len(cmds); i++ {
			key := cmds[i]
			if isExpired(key, index){continue}
			_, ok := shards[index][key]
			if ok {
				nbrExist++
			} 
		}
		return resp.IntResponse(nbrExist)
	}
 	func HandleExpire(cmds []string, index int, isReplay bool) []byte {
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		removeIfExpired(key, index)
		data, ok := shards[index][key]
		if ok {
			incKeyVersion(key)
			exp, err := strconv.Atoi(cmds[2])
			if err != nil {
				fmt.Println(err)
				return resp.ErrorResponse("value is not an integer")
			} else {
				duration := time.Duration(exp) * time.Second
				data.expire = time.Now().Add(duration)
				logToAOF(cmds, isReplay)
				shards[index][key] = data
				return resp.IntResponse(1)
			}
		} else {
			return resp.IntResponse(0)
		}
	}
 	func HandleTTL(cmds []string, index int, isReplay bool) []byte {
		shardLocks[index].RLock()
		defer shardLocks[index].RUnlock()
		key := cmds[1]
		if isExpired(key, index){return resp.IntResponse(-2)}
		data, ok := shards[index][key]
		if ok {
			if data.expire.IsZero() {
				return resp.IntResponse(-1)
			} else {
				return resp.IntResponse(int(time.Until(data.expire).Seconds()))
			}
		} else {
				return resp.IntResponse(-2)
		}
	}
 	func HandleExpireAt(cmds []string, index int, isReplay bool) []byte {
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		removeIfExpired(key, index)
		data, ok := shards[index][key]
		if ok {
			incKeyVersion(key)
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
	}
 	func HandleDBSize() []byte {
		keys := getDBSize() 
		return resp.IntResponse(keys)
	}
  	func HandleKeys(cmds []string, index int, isReplay bool) []byte { 
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
	}
	func HandleFlushAll() []byte { 
		for i := 0; i < NumShards; i++ {
			shardLocks[i].Lock()
				clear(shards[i])
			shardLocks[i].Unlock()
		}
		return resp.OkResponse()
	}
	func HandleLPush(cmds []string, index int, isReplay bool) []byte { 
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		removeIfExpired(key, index)
		data, ok := shards[index][key]
		var newValues []string
		for i:=2; i<len(cmds); i++{
			newValues = append(newValues,cmds[i])
				}
		slices.Reverse(newValues)
		if ok {
			if data.kind != "list"{
				return resp.ErrorResponse("value not a list")
			}   
			incKeyVersion(key)
			*(data.listVal) = append(newValues,*data.listVal...)
		} else {
				data = entry{"list",nil,&newValues,nil,nil, time.Time{}}
				shards[index][key] = data
		}
		shards[index][key] = data
		logToAOF(cmds,isReplay)
		return resp.IntResponse(len(*(data.listVal)))
	}
	func HandleRPush(cmds []string, index int, isReplay bool) []byte { 
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		removeIfExpired(key, index)
		data, ok := shards[index][key]
		var newValues []string
		for i:=2; i<len(cmds); i++{
			newValues = append(newValues,cmds[i])
				}
		if ok {
			if data.kind != "list"{
				return resp.ErrorResponse("value not a list")
			}
			incKeyVersion(key)
			*(data.listVal) = append(*data.listVal,newValues...)
		} else {
			data = entry{"list",nil,&newValues,nil,nil, time.Time{}}
			shards[index][key] = data
		}
		shards[index][key] = data
		logToAOF(cmds,isReplay)
		return resp.IntResponse(len(*(data.listVal)))
	}
	func HandleLRange(cmds []string, index int, isReplay bool) []byte { 
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
			}
 	func HandleLLen(cmds []string, index int, isReplay bool) []byte { 
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
	}
 	func HandleLPop(cmds []string, index int, isReplay bool) []byte { 
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
					incKeyVersion(key)
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
				incKeyVersion(key)
				removedVals = (*data.listVal)[:count]
				(*data.listVal) = (*data.listVal)[count:]
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
	}

	func HandleRPop(cmds []string, index int, isReplay bool) []byte { 
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
					incKeyVersion(key)
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
				incKeyVersion(key)
				removedVals = (*data.listVal)[len(*data.listVal)-count:]
				(*data.listVal) = (*data.listVal)[:len(*data.listVal)-count]
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
	}
	func HandleLPushX(cmds []string, index int, isReplay bool) []byte { 
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		removeIfExpired(key, index)
		data, ok := shards[index][key]
		var newValues []string
		for i:=2; i<len(cmds); i++{
			newValues = append(newValues, cmds[i])
				}
		if ok {
				if data.kind != "list"{
					return resp.ErrorResponse("value not a list")
				}
				incKeyVersion(key)
				slices.Reverse(newValues)   
				*(data.listVal) = append(newValues,*data.listVal...)
				shards[index][key] = data
				logToAOF(cmds,isReplay)
				return resp.IntResponse(len(*data.listVal))
		} else {
			return resp.IntResponse(0)
		}
	}
	func HandleRPushX(cmds []string, index int, isReplay bool) []byte { 
		shardLocks[index].Lock()
		defer shardLocks[index].Unlock()
		key := cmds[1]
		removeIfExpired(key, index)
		data, ok := shards[index][key]
		var newValues []string
		for i:=2; i<len(cmds); i++{
			newValues = append(newValues, cmds[i])
				}
		if ok {
				if data.kind != "list"{
					return resp.ErrorResponse("value not a list")
				}
				incKeyVersion(key)
				*(data.listVal) = append(*data.listVal,newValues...)
				shards[index][key] = data
				logToAOF(cmds,isReplay)
				return resp.IntResponse(len(*data.listVal))
		} else {
			return resp.IntResponse(0)
		}
	}
	func HandleSAdd(cmds []string, index int, isReplay bool) []byte { 
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
		incKeyVersion(key)
		setlen := 0
		for i:=2; i<len(cmds);i++{
		if _, ok := (*data.setVal)[cmds[i]]; !ok{
			(*data.setVal)[cmds[i]] = struct{}{}
			setlen++
			}
		}
		shards[index][key] = data
		logToAOF(cmds,isReplay)
		return resp.IntResponse(setlen)
	}
	func HandleSMembers(cmds []string, index int, isReplay bool) []byte {  
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
	}
	func HandleSIsMember(cmds []string, index int, isReplay bool) []byte {  
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
	}
	func HandleSCard(cmds []string, index int, isReplay bool) []byte {  
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
	}
 	func HandleSRem(cmds []string, index int, isReplay bool) []byte {  
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
		incKeyVersion(key)			
		removedNbr := 0
		for i:=2; i<len(cmds); i++{
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
	}
	func HandleHSet(cmds []string, index int, isReplay bool) []byte {  
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
			incKeyVersion(key)
			exists := false
			if _, ok := (*data.hashVal)[field]; ok {
            	exists = true
        	}
			(*data.hashVal)[field] = val
			shards[index][key] = data
			logToAOF(cmds, isReplay)
			if exists {
				return resp.IntResponse(0)
			}
			return resp.IntResponse(1)
		}else{
			hashVal := make(map[string]string)
			hashVal[field] = val
			shards[index][key] = entry{"hash",nil,nil,&hashVal,nil,time.Time{}}
			logToAOF(cmds,isReplay)
			return resp.IntResponse(1)
		}
	}
	func HandleHGet(cmds []string, index int, isReplay bool) []byte {  
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
	}
	func HandleHGetAll(cmds []string, index int, isReplay bool) []byte {  
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
	}
	func HandleHDel(cmds []string, index int, isReplay bool) []byte {  
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
			return resp.ErrorResponse("value not a hash")
		}
		incKeyVersion(key)
		for i:= 2; i<len(cmds); i++{
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
	}
	func HandleHExists(cmds []string, index int, isReplay bool) []byte {  
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
		}
		return resp.IntResponse(0)
	}
	func HandleHLen(cmds []string, index int, isReplay bool) []byte {  
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
	}
	func HandleLIndex(cmds []string, index int, isReplay bool) []byte {
		key := cmds[1]
		pos , err := strconv.Atoi(cmds[2]) 
		if err != nil {
			fmt.Println(err)
			return resp.ErrorResponse("value is not an integer")
			}
		shardLocks[index].RLock()			
		defer shardLocks[index].RUnlock()
		data, ok := shards[index][key]
		if !ok{
			return resp.NullResponse()
		}
		if data.kind != "list"{
			return resp.ErrorResponse("value not a list")
		}
		size := len(*data.listVal)
		if isExpired(key, index) || size == 0 || pos  >= size || pos * -1  > size  {
			return resp.NullResponse()
		}	
		if pos  < 0 {
			pos = size + pos
			return resp.BulkResponse((*data.listVal)[pos])
		}
		return resp.BulkResponse((*data.listVal)[pos ])
	}
	func HandleLSet(cmds []string, index int, isReplay bool) []byte {
		key := cmds[1]
		pos , err := strconv.Atoi(cmds[2]) 
		val := cmds[3]
		if err != nil {
			fmt.Println(err)
			return resp.ErrorResponse("value is not an integer")
			}
		shardLocks[index].Lock()			
		defer shardLocks[index].Unlock()
		data, ok := shards[index][key]
		if !ok{
			return resp.ErrorResponse("no such key")
		}
		if data.kind != "list"{
			return resp.ErrorResponse("value not a list")
		}
		size := len(*data.listVal)
		if isExpired(key, index) || size == 0 {
			return resp.ErrorResponse("no such key")
		}
		if pos  >= size || pos * -1  > size  {
			return resp.ErrorResponse("index out of range")
		}	
		incKeyVersion(key)
		if pos  < 0 {
			pos = size + pos
		}
		(*data.listVal)[pos] = val
		shards[index][key] = data
		logToAOF(cmds,isReplay)
		return resp.OkResponse()
	}

	func HandleLTrim(cmds []string, index int, isReplay bool) []byte {
		key := cmds[1]
		start , err := strconv.Atoi(cmds[2]) 
		if err != nil {
			fmt.Println(err)
			return resp.ErrorResponse("value is not an integer")
			}
		stop , err := strconv.Atoi(cmds[3]) 
		if err != nil {
			fmt.Println(err)
			return resp.ErrorResponse("value is not an integer")
			}
		shardLocks[index].Lock()			
		defer shardLocks[index].Unlock()
		data, ok := shards[index][key]
		if !ok{
			return resp.OkResponse()
		}
		if data.kind != "list"{
			return resp.ErrorResponse("value not a list")
		}
		incKeyVersion(key)
		size := len(*data.listVal)
		if isExpired(key, index) || size == 0 {
			return resp.OkResponse()
		}
		logToAOF(cmds,isReplay)
		if start  < 0 {
			start = size + start
		}
		if stop  < 0 {
			stop = size + stop
		}
		if stop >= size { stop = size - 1 }
		if start > stop || start >= size || start < 0 || stop < 0{
			delete(shards[index], key)
			return resp.OkResponse()
		}
		if start == stop{
			*data.listVal = []string{(*data.listVal)[start]}
		}else{
			*data.listVal = (*data.listVal)[start : stop + 1]
		}
		shards[index][key] = data
		return resp.OkResponse()
	}

func incKeyVersion(key string){
	keyVersionsMu.Lock()
	keyVersions[key]++
	keyVersionsMu.Unlock()
}