package main

import ("fmt"
 		"net"
 		"bufio"
		"os")

func main(){
	for {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println(err)
		break
	}
	userInput, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		fmt.Println(err)
            conn.Close()
            break
        }
	fmt.Fprint(conn, userInput)
	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println(err)
            conn.Close()
            break
        }
	fmt.Println(response)
	conn.Close()
	}
}



