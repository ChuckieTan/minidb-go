package util

import (
	"bytes"
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"
)

type MyFormatter struct{}

func (m *MyFormatter) Format(entry *log.Entry) ([]byte, error) {
	// 设置 log level 颜色
	levelColor := color.FgCyan
	switch entry.Level {
	case log.DebugLevel, log.TraceLevel:
		levelColor = color.FgCyan
	case log.WarnLevel:
		levelColor = color.FgYellow
	case log.ErrorLevel, log.FatalLevel, log.PanicLevel:
		levelColor = color.FgRed
	case log.InfoLevel:
		levelColor = color.FgGreen
	default:
		levelColor = color.FgBlue
	}

	toColor := color.New(levelColor).SprintFunc()
	timestamp := entry.Time.Format("2006-01-02 15:04:05.000")
	var file string
	var line int
	if entry.Caller != nil {
		file = filepath.Base(entry.Caller.File)
		line = entry.Caller.Line
	}
	levelText := strings.ToLower(entry.Level.String())

	msg := fmt.Sprintf("[%s] [%s:%d] [GOID:%d] [%s] %s\n", timestamp, file, line, getGID(), toColor(levelText), entry.Message)
	return []byte(msg), nil
}

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}
