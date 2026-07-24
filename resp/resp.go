package resp

import (
	"fmt"
	"strings"
)

func EncodeCommand(cmds []string) []byte {
	var response []byte
	var str_reponse strings.Builder
	str_reponse.WriteString(fmt.Sprintf("*%d\r\n", len(cmds)))
	for i := 0; i < len(cmds); i++ {
		str_reponse.WriteString(fmt.Sprintf("$%d\r\n", len(cmds[i])))
		str_reponse.WriteString(fmt.Sprintf("%s\r\n", cmds[i]))
	}
	response = []byte(str_reponse.String())
	return response
}
