package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var logInputChannel = make(chan LogInfo, 10)

type LogLevel string

const (
	Error LogLevel = "E"
	Panic          = "P"
	Info           = "I"
)

// LogInfo 传递Log信息的结构体
type LogInfo struct {
	level      LogLevel
	timeString string
	message    string
}

func GenerateInfoLog(message string) {
	log := LogInfo{
		level:      Info,
		timeString: time.Now().Format("2006-01-02 15:04:05"),
	}
	pc, _, _, ok := runtime.Caller(1)
	if !ok {
		log.message = "Can not Get Caller Function, Message: " + message
	} else {
		log.message = runtime.FuncForPC(pc).Name() + " :" + message
	}
	logInputChannel <- log
	return
}

func GenerateErrorLog(isPanic bool, needStackTrace bool, message string, keyParams ...string) {
	log := LogInfo{
		timeString: time.Now().Format("2006-01-02 15:04:05"),
	}
	if isPanic {
		log.level = Panic
	} else {
		log.level = Error
	}
	param := strings.Join(keyParams, " ")
	if needStackTrace { // 需要全部堆栈信息
		log.message = "Message: " + message + ", parameters: " + param + "\n"
		log.message += "Stack Trace: \n"

		pcs := make([]uintptr, 100)
		n := runtime.Callers(1, pcs)
		pcs = pcs[:n]
		frames := runtime.CallersFrames(pcs)

		for frame, more := frames.Next(); more; frame, more = frames.Next() {
			log.message += frame.File + ": " + strconv.Itoa(frame.Line) + ", Function: " + frame.Function + "\n"
		}
	} else { // 不需要
		pc, _, _, ok := runtime.Caller(1)
		if !ok {
			log.message = "Can not Get Caller Function, Message: " + message + ", parameters: " + param
		} else {
			log.message = runtime.FuncForPC(pc).Name() + " :" + message + ", parameters: " + param
		}
	}

	logInputChannel <- log
}

func (li *LogInfo) toByteArray() []byte {
	return []byte(fmt.Sprintf("%s %s: %s \n", li.level, li.timeString, li.message))
}

// Logger 记录log信息的结构体
type Logger struct {
	loggerFile *os.File

	isStop bool
}

// NewLogger 传入log文件存储的路径 以获取一个新的Logger
func NewLogger(logPath string) (*Logger, error) {
	result := &Logger{}
	f, e := os.OpenFile(GenerateLogFilePath(logPath), os.O_CREATE|os.O_RDWR, 0644)
	if e != nil {
		return nil, e
	}
	result.loggerFile = f
	result.ListenLoggerChannel()
	return result, e
}

func (logger *Logger) StopLogger() {
	logger.isStop = true
}

// ListenLoggerChannel 开始监听channel以接收log信息 写入log文件并且打印
func (logger *Logger) ListenLoggerChannel() {
	go func() {
		var (
			log   LogInfo
			bytes []byte
			e     error
		)
		for { // 循环监听
			select {
			case log = <-logInputChannel:
				// 记录log
				bytes = log.toByteArray()
				_, e = logger.loggerFile.Write(bytes)
				if e != nil { // 写入logger失败 准备关闭logger
					fmt.Println("Can Not Write Log Cause of: ", e.Error())
					fmt.Println("Will Close Logger!")
					logger.isStop = true
				}
				fmt.Println(string(log.level) + " " + log.timeString + " " + log.message)
			default:
				if logger.isStop {
					close(logInputChannel) // 销毁channel
					e = logger.loggerFile.Sync()
					if e != nil {
						fmt.Println("Can Not Sync Log File Cause of: ", e.Error())
					}
					e = logger.loggerFile.Close() // 关闭文件
					if e != nil {
						fmt.Println("Can Not Close Log File Cause of: ", e.Error())
					}
					return
				}
			}
		}
	}()
}

func GenerateLogFilePath(path string) string {
	fileName := "log." + time.Now().Format("2006_01_02_15_04_05") + ".misaka"
	return filepath.Join(path, fileName)
}
