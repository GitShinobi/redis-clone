package main

import ("fmt"
 		"net"
		"strings"
		"strconv"
		"io"
		"bufio"
		"sync"
		"time")
type entry struct {
	val    string
	expire time.Time
}
var store = make(map[string]entry)
var mu sync.Mutex
func main(){
	listener, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		fmt.Println(err)
	}
	defer listener.Close()
	fmt.Println("listening on :8080")
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
		response := writeResponse(cmds)
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

func writeResponse(cmds []string)([]byte){
	var response []byte
	
	switch cmds[0] {
		case "PING":
			response = []byte("+PONG\r\n")
		case "SET":
			mu.Lock()
			key := cmds[1]
			val := cmds[2]
			store[key] = entry{val,time.Time{}} 
			mu.Unlock()
			response = []byte("+OK\r\n")
		case "GET" :
			mu.Lock()
			key := cmds[1]
			removeIfExpired(key)
			data, ok := store[key]
			mu.Unlock()
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
			mu.Unlock()
			var str_reponse string
			if ok{
				delete(store,key)
				str_reponse = ":1\r\n"
			}else {
				str_reponse = ":0\r\n"
			}
			response = []byte(str_reponse)
		case "EXISTS":
			mu.Lock()
			key := cmds[1]
			removeIfExpired(key)
			_, ok := store[key]
			mu.Unlock()
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
					response =  []byte("-ERR value is not an integer\r\n")
				}else{
					duration := time.Duration(exp) * time.Second
					data.expire = time.Now().Add(duration)
					store[key] = data
					str_reponse = ":1\r\n"
				}
			}else {
				str_reponse = ":0\r\n"
			}
			mu.Unlock()
			response = []byte(str_reponse)
		case "TTL" :
			mu.Lock()
			key := cmds[1]
			removeIfExpired(key)
			data, ok := store[key]
			mu.Unlock()
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