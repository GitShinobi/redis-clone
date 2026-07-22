package main

import ("fmt"
 		"net"
		"log"
		"strings"
		"strconv"
		"io"
		"bufio")

func main(){
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	fmt.Println("listening on :8080")
	for {
		conn, err := listener.Accept()
		if err != nil {
		log.Println(err)
			return 
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn)(){
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		str,err := readCommand(reader)
		if err != nil {
			return
		}
		fmt.Println(str)
	}
}

func readCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	nbr_cmd,err := strconv.Atoi(GetCmdSize(line))
	if err != nil {
		fmt.Println("failed conversion")
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
		fmt.Println("failed conversion")
		fmt.Println(err)
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
