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

func ErrorMsg(size int, num int, msg string)([]byte){
	var response []byte
 	if num !=  size {
		response = []byte(msg)  
	}
	return response
}