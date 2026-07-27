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
)

type entry struct {
	val    string | []string | map[string]string
	expire time.Time
}

var shards [16]map[string]entry
var shardLocks [16]sync.RWMutex
var cmdBuffer [][]string
var cmdBufMu sync.Mutex

func init() {
	for i := 0; i < 16; i++ {
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
	aof, err := os.ReadFile("aof.log")
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
			time.Sleep(24 * time.Hour)
		}
	}()
	go func() {
		for {
			for i := 0; i < 16; i++ {
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
			if len(cmdBuffer) > 10 {
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
		fmt.Println("failed conversion : ", err)
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
	var response []byte
	var index int
	var strResponse string
	cmdsLen := len(cmds)
	if cmdsLen > 1 {
		index = shardIndex(cmds[1], 16)
	}
	switch strings.ToUpper(cmds[0]) {
	case "PING":
		response = []byte("+PONG\r\n")
	case "SET":
		syntaxError := resp.ErrorMsg(cmdsLen, 3, "-ERR Syntaxe command\r\n")
		if syntaxError != nil{
			return syntaxError
		}

		shardLocks[index].Lock()
		key := cmds[1]
		val := cmds[2]
		shards[index][key] = entry{val, time.Time{}}
		shardLocks[index].Unlock()
		cmdBufMu.Lock()
		if !isReplay {
			cmdBuffer = append(cmdBuffer, cmds)
		}
		cmdBufMu.Unlock()
		response = []byte("+OK\r\n")
	case "GET":
		syntaxError := resp.ErrorMsg(cmdsLen, 2, "-ERR Syntaxe command\r\n")
		if syntaxError != nil{
			return syntaxError
		}
		shardLocks[index].RLock()
		key := cmds[1]
		data, ok := shards[index][key]
		shardLocks[index].RUnlock()
		if ok {
			size := len([]byte(data.val))
			strResponse = fmt.Sprintf("$%d\r\n%s\r\n", size, data.val)
		} else {
			strResponse = "$-1\r\n"
		}
		response = []byte(strResponse)
	case "DEL":
		syntaxError := resp.ErrorMsg(cmdsLen, 2, "-ERR Syntaxe command\r\n")
		if syntaxError != nil{
			return syntaxError
		}
		shardLocks[index].Lock()
		key := cmds[1]
		removeIfExpired(key, index)
		_, ok := shards[index][key]
		if ok {
			cmdBufMu.Lock()
			if !isReplay {
				cmdBuffer = append(cmdBuffer, cmds)
			}
			cmdBufMu.Unlock()
			delete(shards[index], key)
			strResponse = ":1\r\n"
		} else {
			strResponse = ":0\r\n"
		}
		shardLocks[index].Unlock()
		response = []byte(strResponse)
	case "EXISTS":
		syntaxError := resp.ErrorMsg(cmdsLen, 2, "-ERR Syntaxe command\r\n")
		if syntaxError != nil{
			return syntaxError
		}
		shardLocks[index].RLock()
		key := cmds[1]
		_, ok := shards[index][key]
		shardLocks[index].RUnlock()
		if ok {
			strResponse = ":1\r\n"
		} else {
			strResponse = ":0\r\n"
		}
		response = []byte(strResponse)
	case "EXPIRE":
		syntaxError := resp.ErrorMsg(cmdsLen, 3, "-ERR Syntaxe command\r\n")
		if syntaxError != nil{
			return syntaxError
		}
		shardLocks[index].Lock()
		key := cmds[1]
		removeIfExpired(key, index)
		data, ok := shards[index][key]
		if ok {
			exp, err := strconv.Atoi(cmds[2])
			if err != nil {
				fmt.Println(err)
				strResponse = "-ERR value is not an integer\r\n"
			} else {
				duration := time.Duration(exp) * time.Second
				data.expire = time.Now().Add(duration)
				cmdBufMu.Lock()
				if !isReplay {
					cmdBuffer = append(cmdBuffer, cmds)
				}
				cmdBufMu.Unlock()
				shards[index][key] = data
				strResponse = ":1\r\n"
			}
		} else {
			strResponse = ":0\r\n"
		}
		shardLocks[index].Unlock()
		response = []byte(strResponse)
	case "TTL":
		syntaxError := resp.ErrorMsg(cmdsLen, 2, "-ERR Syntaxe command\r\n")
		if syntaxError != nil{
			return syntaxError
		}
		shardLocks[index].RLock()
		key := cmds[1]
		data, ok := shards[index][key]
		shardLocks[index].RUnlock()
		if ok {
			if data.expire.IsZero() {
				strResponse = ":-1\r\n"
			} else {
				strResponse = fmt.Sprintf(":%d\r\n", int(data.expire.Sub(time.Now()).Seconds()))
			}
		} else {
			strResponse = ":-2\r\n"
		}
		response = []byte(strResponse)
	case "EXPIREAT":
		syntaxError := resp.ErrorMsg(cmdsLen, 3, "-ERR Syntaxe command\r\n")
		if syntaxError != nil{
			return syntaxError
		}
		shardLocks[index].Lock()
		key := cmds[1]
		removeIfExpired(key, index)
		data, ok := shards[index][key]
		if ok {
			exp, err := strconv.Atoi(cmds[2])
			if err != nil {
				fmt.Println(err)
				strResponse = "-ERR value is not an integer\r\n"
			} else {
				data.expire = time.Unix(int64(exp), 0)
				shards[index][key] = data
				strResponse = ":1\r\n"
			}
		} else {
			strResponse = ":0\r\n"
		}
		shardLocks[index].Unlock()
		response = []byte(strResponse)
	case "DBSIZE" : 
	keys := getDBSize() 
	strResponse = fmt.Sprintf(":%d\r\n",keys)
	response = []byte(strResponse)
	case "KEYS" : 
	size :=  getDBSize() 
	if size == 0{
			strResponse = "*0\r\n"
			return []byte(strResponse)
	}

	keys :=  fmt.Sprintf("*%d\r\n",size)
	for i := 0; i < 16; i++ {
		shardLocks[i].RLock()
			for k, _ := range shards[i] {
				keys = keys + fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
			}
		shardLocks[i].RUnlock()
	}
	response = []byte(keys)
	default:
		response = []byte("-ERR unknown command\r\n")
	}
	return response
}
func removeIfExpired(key string, index int) {
	data, ok := shards[index][key]
	if ok && !data.expire.IsZero() && data.expire.Before(time.Now()) {
		delete(shards[index], key)
	}
}

func rewriteAOF() {
	var content []byte
	for i := 0; i < 16; i++ {
		shardLocks[i].Lock()
		snapshot := make(map[string]entry, len(shards[i]))
		for k, v := range shards[i] {
			snapshot[k] = v
		}
		shardLocks[i].Unlock()
		var timeCmd string
		for key, entry := range snapshot {
			if !entry.expire.IsZero() && entry.expire.Before(time.Now()) {
				continue
			}
			valCmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(entry.val), entry.val)
			content = append(content, []byte(valCmd)...)
			if !entry.expire.IsZero() {
				timeCmd = fmt.Sprintf("*3\r\n$8\r\nEXPIREAT\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(strconv.Itoa(int(entry.expire.Unix()))), strconv.Itoa(int(entry.expire.Unix())))
				content = append(content, []byte(timeCmd)...)
			}
		}
	}
	err := os.WriteFile("aof.log", content, 0666)
	if err != nil {
		fmt.Println(err)
	}

}

func appendCommand(cmdBuffer [][]string, name string) {
	f, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}
	for i := range cmdBuffer {
		cmds := resp.EncodeCommand(cmdBuffer[i])
		if _, err := f.Write(cmds); err != nil {
			f.Close()
			fmt.Println(err)
		}
	}

	if err := f.Close(); err != nil {
		fmt.Println(err)
	}
}
func shardIndex(key string, numShards int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % numShards
}

func getDBSize() int{
	keys := 0
	for i := 0; i < 16; i++ {
		shardLocks[i].RLock()
		keys += len(shards[i])
		shardLocks[i].RUnlock()
	} 
	return keys
}