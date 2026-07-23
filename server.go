package main

import ("fmt"
 		"net"
		"strings"
		"strconv"
		"io"
		"os"
		"bufio"
		"sync"
		"time"
		"bytes")
type entry struct {
	val    string
	expire time.Time
}
var store = make(map[string]entry)
var mu sync.RWMutex

func main(){
	listener, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		fmt.Println(err)
	}
	defer listener.Close()
	fmt.Println("listening on :8080")
	aof,err := os.ReadFile("aof.log")
	if err != nil{
	fmt.Println("failed aof loading : ",err)
	}else{
		byteReader := bytes.NewReader(aof)
		reader := bufio.NewReader(byteReader)
		for {
		cmds,err := readCommand(reader)
		if err != nil{
			if err == io.EOF{
				break
			}else{
				fmt.Println("failed aof recovering : ",err)
				return
			}
		} 
	writeResponse(cmds,true)
	}
	} 
	go func(){
		for {
			rewriteAOF()
			time.Sleep(24 * time.Hour)
		}
	}()
	go func(){
		for {
			var keys []string
			mu.Lock()
			for k:= range store {
			keys = append(keys,k)}
			for _,k:=range keys{
				removeIfExpired(k)
			}
			mu.Unlock()
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

func handleConnection(conn net.Conn)(){
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		cmds,err := readCommand(reader)
		if err != nil {
			if err == io.EOF{
				fmt.Println("client disconnected : ",err)
			}else{
				fmt.Println("failed reading command : ",err)
			}
			return
		}
		if len(cmds)==0{
			fmt.Println("no command found")
		return 
			}
		response := writeResponse(cmds,false)
		conn.Write(response)
	}
}

func readCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	nbr_cmd,err := strconv.Atoi(GetCmdSize(line))
	if err != nil {
		fmt.Println("failed conversion : ",err)
		fmt.Println(err)
		return nil, err
	}
	var cmds []string
	for i:= 0; i < nbr_cmd; i++{
		size, err := reader.ReadString('\n')
		if err != nil {
		return nil, err
		}
		size_val,err := strconv.Atoi(GetCmdSize(size))
		if err != nil {
		fmt.Println("failed conversion : ",err)
		return nil, err
		}
		cmd_buf := make([]byte,size_val)
		_,err = io.ReadFull(reader,cmd_buf)
		if err != nil {
		return nil, err
		}
		
		cmd_val := string(cmd_buf)
		cmds = append(cmds,cmd_val)

		_, err = reader.ReadString('\n')
		if err != nil {
		return nil, err
		}
	}
	return cmds,nil
}

func GetCmdSize(chunck string)(string){
	str_tab := strings.Split(chunck, "\r\n")
	val_str  := str_tab[0]
	return val_str[1:]
}

func writeResponse(cmds []string,isReplay bool)([]byte){
	var response []byte
	
	switch strings.ToUpper(cmds[0]){
		case "PING":
			response = []byte("+PONG\r\n")
		case "SET":
			mu.Lock()
			key := cmds[1]
			val := cmds[2]
			if !isReplay { appendCommend(cmds,"aof.log")}
			store[key] = entry{val,time.Time{}} 
			mu.Unlock()
			response = []byte("+OK\r\n")
		case "GET" :
			mu.RLock()
			key := cmds[1]
			data, ok := store[key]
			mu.RUnlock()
			var str_reponse string
			if ok{
				size := len([]byte(data.val))
				str_reponse = fmt.Sprintf("$%d\r\n%s\r\n",size,data.val)
			}else {
				str_reponse = "$-1\r\n"
			}
			response = []byte(str_reponse)
		case "DEL":
			mu.Lock()
			key := cmds[1]
			removeIfExpired(key)
			_, ok := store[key]
			var str_reponse string
			if ok{
 				if !isReplay { appendCommend(cmds,"aof.log")}
				delete(store,key)
				str_reponse = ":1\r\n"
			}else {
				str_reponse = ":0\r\n"
			}
			mu.Unlock()
			response = []byte(str_reponse)
		case "EXISTS":
			mu.RLock()
			key := cmds[1]
			_, ok := store[key]
			mu.RUnlock()
			var str_reponse string
			if ok{
				str_reponse = ":1\r\n"
			}else {
				str_reponse = ":0\r\n"
			}
			response = []byte(str_reponse)
		case "EXPIRE":
			mu.Lock()
			key := cmds[1]
			removeIfExpired(key)
			data, ok := store[key]
			var str_reponse string
			if ok{
				exp,err := strconv.Atoi(cmds[2])
				if err != nil{
					fmt.Println(err)
					str_reponse =  "-ERR value is not an integer\r\n"
				}else{
					duration := time.Duration(exp) * time.Second
					data.expire = time.Now().Add(duration)
 					if !isReplay {appendCommend([]string{"EXPIREAT",cmds[1],strconv.Itoa(int(data.expire.Unix()))},"aof.log")}
					store[key] = data
					str_reponse = ":1\r\n"
				}
			}else {
				str_reponse = ":0\r\n"
			}
			mu.Unlock()
			response = []byte(str_reponse)
		case "TTL" :
			mu.RLock()
			key := cmds[1]
			data, ok := store[key]
			mu.RUnlock()
			var str_reponse string
			if ok{
				if data.expire.IsZero(){
					str_reponse = ":-1\r\n"
					}else{
						str_reponse = fmt.Sprintf(":%d\r\n",int(data.expire.Sub(time.Now()).Seconds()))
					}
			}else {
				str_reponse = ":-2\r\n"
			}
			response = []byte(str_reponse)
		case "EXPIREAT":
			mu.Lock()
			key := cmds[1]
			removeIfExpired(key)
			data, ok := store[key]
			var str_reponse string
			if ok{
				exp,err := strconv.Atoi(cmds[2])
				if err != nil{
					fmt.Println(err)
					str_reponse = "-ERR value is not an integer\r\n"
				}else{
				data.expire = time.Unix(int64(exp), 0)
				store[key] = data
				str_reponse = ":1\r\n"}
			}else {
				str_reponse = ":0\r\n"
			}
			mu.Unlock()
			response = []byte(str_reponse)
		default:
			response = []byte("-ERR unknown command\r\n")
		}
		return response
}

func removeIfExpired(key string) {
	data, ok := store[key]
	if ok && !data.expire.IsZero() && data.expire.Before(time.Now()){
		delete(store, key)
		}
}

func encodeCommand(cmds []string) []byte{
	var response []byte
	var str_reponse strings.Builder
	str_reponse.WriteString( fmt.Sprintf("*%d\r\n",len(cmds)))
	for i:=0; i<len(cmds); i++ {
		str_reponse.WriteString(fmt.Sprintf("$%d\r\n",len(cmds[i])))
		str_reponse.WriteString(fmt.Sprintf("%s\r\n",cmds[i]))
	}
	response = []byte(str_reponse.String())
	return response
}

func rewriteAOF(){
	mu.Lock()
	snapshot := make(map[string]entry, len(store))
	for k, v := range store {
    snapshot[k] = v
}
	mu.Unlock()
	var content []byte
	var time_cmd string
	for key, entry := range snapshot {
		if !entry.expire.IsZero() && entry.expire.Before(time.Now()) {
        continue
   		 }
		val_cmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",len(key),key,len(entry.val),entry.val)
		content = append(content,[]byte(val_cmd)...)
		if !entry.expire.IsZero() {
			time_cmd = fmt.Sprintf("*3\r\n$8\r\nEXPIREAT\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",len(key),key,len(strconv.Itoa(int(entry.expire.Unix()))),strconv.Itoa(int(entry.expire.Unix())))
			content = append(content,[]byte(time_cmd)...)
		}
	}
	err := os.WriteFile("aof.log",content, 0666)
	if err != nil {
		fmt.Println(err)
	}
}

func appendCommend(cmds []string, name string){
	content := encodeCommand(cmds)
	f,err := os.OpenFile(name, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}
	if _, err := f.Write(content); err != nil {
		f.Close() 
		fmt.Println(err)
	}
	if err := f.Close(); err != nil {
		fmt.Println(err)
	}
}