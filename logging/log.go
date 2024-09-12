package logging

import (
	"fmt"
	"log"
)

// Logger 系统日志
type Logger interface {
	Info(format string, a ...interface{})
	Error(format string, a ...interface{})
	Debug(format string, a ...interface{})
}

type DefaultLogger struct {
}

func (l *DefaultLogger) Info(format string, a ...interface{}) {
	fmt.Println(fmt.Sprintf(format, a...))
}

func (l *DefaultLogger) Error(format string, a ...interface{}) {
	log.Println(fmt.Sprintf(format, a...))
}

func (l *DefaultLogger) Debug(format string, a ...interface{}) {
	log.Println(fmt.Sprintf(format, a...))
}
