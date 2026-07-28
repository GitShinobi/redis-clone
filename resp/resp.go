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
		strResponse := fmt.Sprintf("ERR wrong number of arguments for '%s' command\r\n",cmd)
		return []byte(strResponse)  
	}
	return nil
}