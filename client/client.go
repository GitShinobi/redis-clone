package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"redis/resp"
	"strconv"
	"strings"
	"time"
	"os/exec"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println(err)
		return
	}
	stdinReader := bufio.NewReader(os.Stdin)
	conn_reader := bufio.NewReader(conn)
	timeout := 0
	nowFormatted := time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00")
	fmt.Printf("1:M %s # Server initialized\n", nowFormatted)
	fmt.Printf("1:M %s * Ready to accept connections\n", nowFormatted)
	fmt.Println("listening on localhost:8080")
	for {
		fmt.Print("redis:8080> ")
		cmd, err := stdinReader.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			if err == io.EOF {
				conn.Close()
				return
			}
			timeout++
			if timeout == 1000{
				break
			}
			continue
		}
		cmd_tab := strings.Fields(cmd)
		if len(cmd_tab) == 0 {
			continue
		}
		if strings.ToUpper(cmd_tab[0]) == "Q" {
			conn.Close()
			return
		}
		if strings.ToUpper(cmd_tab[0]) == "CLEAR" {
			cmd := exec.Command("clear")
			cmd.Stdout = os.Stdout
			if err := cmd.Run(); err != nil {
     		fmt.Printf("Command failed: %v\n", err)
   			 }
			 continue
		}
		conn.Write(resp.EncodeCommand(cmd_tab))
		stop := 0
		count := 0
		for {
			response, err := conn_reader.ReadString('\n')
			stop++
			if err != nil {
				fmt.Println(err)
				if err == io.EOF {
					fmt.Println("server disconnected")
					conn.Close()
					return
				}
				timeout++
				if timeout == 1000{
				break
			}
				continue
			}
			fmt.Println(response)
			timeout = 0
			if response[0] == '+' || response[0] == '-' || response[0] == ':' || strings.HasPrefix(response, "$-1") || strings.HasPrefix(response, "*-1") {
				break
			}
			strVal := strings.TrimSpace(response[1:])
			if response[0] == '*' {
				num, err := strconv.Atoi(strVal)
				if err != nil {
					fmt.Println(err)
					break
				}
				count = num + 1
			}

			if response[0] == '$' && stop == 1 {
				valLine, err := conn_reader.ReadString('\n')
				if err != nil {
					break
				}
				fmt.Print(valLine)
				break
			}

			if count == stop {
				break
			}
		}
	}
}
