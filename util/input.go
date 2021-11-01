package util

import (
	"fmt"
)

var (
	startUpMessage = "Minidb Version 0.0.1\n"
	fullPragma     = ">>> "
	lightPragma    = "  > "
)

func StartUp() {
	fmt.Print(startUpMessage)
}

func scanLine() string {
	var c byte
	var err error
	var b []byte
	for err == nil {
		_, err = fmt.Scanf("%c", &c)

		if c != '\n' {
			b = append(b, c)
		} else {
			break
		}
	}

	return string(b)
}

func ReadInput() (buffer string) {
	fmt.Print(fullPragma)
	buffer = scanLine()

	for buffer[len(buffer)-1] != ';' {
		line := ""
		fmt.Print(lightPragma)
		line = scanLine()
		buffer += line
	}
	return buffer
}
