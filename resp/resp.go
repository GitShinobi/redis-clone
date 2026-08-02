package resp

import (
	"fmt"
	"strings"
)

func EncodeCommand(cmds []string) []byte {
	var response []byte
	var strResponse strings.Builder
	strResponse.WriteString(fmt.Sprintf("*%d\r\n", len(cmds)))
	for i := 0; i < len(cmds); i++ {
		strResponse.WriteString(fmt.Sprintf("$%d\r\n", len(cmds[i])))
		strResponse.WriteString(fmt.Sprintf("%s\r\n", cmds[i]))
	}
	response = []byte(strResponse.String())
	return response
}

func ErrorMsg(size int, num int, cmd string, op string)([]byte){
	var valid bool
	switch op {
	case "==":
		valid = size == num
	case ">=":
		valid = size >= num
	default:
		return []byte(fmt.Sprintf("-ERR internal error: invalid operator '%s' for command '%s'\r\n", op, cmd))	}
 	  if !valid {
		strResponse := fmt.Sprintf("-ERR wrong number of arguments for '%s' command\r\n",cmd)
		return []byte(strResponse)  
	}
	return nil
}

func okResponse() []byte {
    return []byte("+OK\r\n")
}

func intResponse(n int) []byte {
    return []byte(fmt.Sprintf(":%d\r\n", n))
}

func bulkResponse(s string) []byte {
    return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
}

func nullResponse() []byte {
    return []byte("$-1\r\n")
}

func emptyArrayResponse() []byte {
    return []byte("*0\r\n")
}
func ArrayResponse(n int) []byte {
    return []byte(fmt.Sprintf("*%d\r\n",n))
}

func errorResponse(msg string) []byte {
    return []byte(fmt.Sprintf("-ERR %s\r\n", msg))
}