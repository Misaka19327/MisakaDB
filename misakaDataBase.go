package main

import (
	"MisakaDB/index"
	"MisakaDB/logger"
	"MisakaDB/storage"
	"MisakaDB/util"
	"errors"
	"fmt"
	"github.com/tidwall/redcon"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

// 以下为可选配置项
const (
	MisakaDataBaseFolderPath = "D:\\MisakaDBTest"        // 数据库进行数据持久化时 文件的保存位置 注意该路径下不可以有其他人任何文件
	RecordFileMaxSize        = 65536                     // 文件的最大存储字节数
	RecordFileIOMode         = storage.TraditionalIOFile // 对文件的读写方式 可以是传统的IO 也可以是Mmap
	MisakaServerAddr         = ":23456"                  // 数据库的端口
	LoggerPath               = "D:\\MisakaDBLog"         // 数据库的Log保存位置 该位置下有无其它文件都可以
	SyncDuration             = 1000                      // 持久化文件定时同步的时间间隔 单位为毫秒
)

// 下面这是Linux版的路径 方便我切换
//const (
//	MisakaDataBaseFolderPath = "/home/MisakaDB"
//	LoggerPath               = "/home/MisakaDBLog"
//)

type MisakaDataBase struct {
	server *redcon.Server
	logger *logger.Logger

	hashIndex   *index.HashIndex
	stringIndex *index.StringIndex
	listIndex   *index.ListIndex
}

func Init() (*MisakaDataBase, error) {
	database := &MisakaDataBase{}
	var e error

	// 初始化logger
	database.logger, e = logger.NewLogger(LoggerPath)
	if e != nil {
		return nil, e
	}
	logger.GenerateInfoLog("Logger is Ready!")

	// 读取文件
	activeFiles, archiveFiles, e := storage.RecordFilesInit(MisakaDataBaseFolderPath, RecordFileMaxSize)
	if e != nil {
		return nil, e
	}

	// 开始构建索引
	for key, value := range activeFiles {
		if key == storage.Hash {
			database.hashIndex, e = index.BuildHashIndex(value, archiveFiles[storage.Hash], RecordFileIOMode, MisakaDataBaseFolderPath, RecordFileMaxSize, time.Millisecond*SyncDuration)
			if e != nil {
				return nil, e
			}
			logger.GenerateInfoLog("Hash Index is Ready!")
		}

		if key == storage.String {
			database.stringIndex, e = index.BuildStringIndex(value, archiveFiles[storage.String], RecordFileIOMode, MisakaDataBaseFolderPath, RecordFileMaxSize, time.Millisecond*SyncDuration)
			if e != nil {
				return nil, e
			}
			logger.GenerateInfoLog("String Index is Ready!")
		}

		if key == storage.List {
			database.listIndex, e = index.BuildListIndex(value, archiveFiles[storage.List], RecordFileIOMode, MisakaDataBaseFolderPath, RecordFileMaxSize, time.Millisecond*SyncDuration)
			if e != nil {
				return nil, e
			}
			logger.GenerateInfoLog("List Index is Ready! ")
		}
	}

	// 开始检查索引是否构建 如果否 构建一个空的索引
	// 这是防activeFiles本身不存在
	if database.hashIndex == nil {
		database.hashIndex, e = index.BuildHashIndex(nil, nil, RecordFileIOMode, MisakaDataBaseFolderPath, RecordFileMaxSize, time.Millisecond*SyncDuration)
		if e != nil {
			logger.GenerateErrorLog(false, false, e.Error(), "Build Empty Hash Index Failed!")
			return nil, e
		}
		logger.GenerateInfoLog("Hash Index is Ready!")
	}
	if database.stringIndex == nil {
		database.stringIndex, e = index.BuildStringIndex(nil, nil, RecordFileIOMode, MisakaDataBaseFolderPath, RecordFileMaxSize, time.Millisecond*SyncDuration)
		if e != nil {
			logger.GenerateErrorLog(false, false, e.Error(), "Build Empty String Index Failed!")
			return nil, e
		}
		logger.GenerateInfoLog("String Index is Ready!")
	}
	if database.listIndex == nil {
		database.listIndex, e = index.BuildListIndex(nil, archiveFiles[storage.List], RecordFileIOMode, MisakaDataBaseFolderPath, RecordFileMaxSize, time.Millisecond*SyncDuration)
		if e != nil {
			return nil, e
		}
		logger.GenerateInfoLog("List Index is Ready! ")
	}

	// 初始化服务器
	e = database.ServerInit()
	if e != nil {
		logger.GenerateErrorLog(false, false, e.Error(), "Server Init Failed!")
		return nil, e
	}
	logger.GenerateInfoLog("Server is Ready!")

	return database, nil
}

func (db *MisakaDataBase) Destroy() error {

	// 关闭服务器
	e := db.server.Close()
	if e != nil {
		logger.GenerateErrorLog(false, false, e.Error())
		return e
	}

	// 关闭索引 索引里会挨个关闭文件的
	e = db.hashIndex.CloseIndex()
	if e != nil {
		return e
	}
	e = db.stringIndex.CloseIndex()
	if e != nil {
		return e
	}
	e = db.listIndex.CloseIndex()
	if e != nil {
		return e
	}

	// 关闭logger
	db.logger.StopLogger()

	return nil
}

func (db *MisakaDataBase) ServerInit() error {
	// redcon是多线程的 而且应该是线程安全的

	// 创建一个Server需要三个回调函数：
	// 1 通过连接接收请求时调用的函数
	// 2 接受连接时调用的函数
	// 3 断开连接时调用的函数
	var (
		e       error
		expired int
	)
	db.server = redcon.NewServer(MisakaServerAddr,
		func(conn redcon.Conn, cmd redcon.Command) {

			// 捕捉panic
			defer func() {
				p := recover()
				if p != nil {
					stackTrace := debug.Stack() // 获取引发panic位置的堆栈信息
					logger.GenerateErrorLog(true, false, string(stackTrace), fmt.Sprintf("%v", p))
				}
				return
			}()

			logger.GenerateInfoLog(conn.RemoteAddr() + ": Query Command: " + util.TurnByteArray2ToString(cmd.Args))

			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				// 命令不能识别
				conn.WriteError("ERR unknown command '" + util.TurnByteArray2ToString(cmd.Args) + "'")
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Unknown Query: " + util.TurnByteArray2ToString(cmd.Args))
				return
			case "ping":
				conn.WriteString("PONG")
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: ping")
				return
			case "quit":
				conn.WriteString("OK")
				e = conn.Close()
				if e != nil {
					logger.GenerateErrorLog(false, false, e.Error())
				}
				return

			// string部分的命令解析
			case "set":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: set")
				if len(cmd.Args) == 3 {
					// set key value
					e = db.stringIndex.Set(cmd.Args[1], cmd.Args[2], -1)
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString("OK")
				} else if len(cmd.Args) == 5 {
					// set key value ex/px time
					expired, e = strconv.Atoi(string(cmd.Args[4]))
					if e != nil {
						conn.WriteError("Cannot Read Expired As Number: " + e.Error())
						return
					}
					var expiredAt int64
					expiredAt, e = util.CalcTimeUnix(string(cmd.Args[3]), expired)
					if e != nil {
						conn.WriteError(e.Error() + string(cmd.Args[3]))
						return
					}
					e = db.stringIndex.Set(cmd.Args[1], cmd.Args[2], expiredAt)
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString("OK")
					return
				} else {
					// 参数数量错误
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "setnx":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: setnx")
				if len(cmd.Args) == 3 {
					// setnx key value
					e = db.stringIndex.SetNX(cmd.Args[1], cmd.Args[2], -1)
					if e != nil {
						if errors.Is(logger.KeyIsExisted, e) {
							conn.WriteInt(0)
							return
						} else {
							conn.WriteError(e.Error())
							return
						}
					}
					conn.WriteInt(1)
				} else if len(cmd.Args) == 5 {
					// setnx key value ex/px time
					expired, e = strconv.Atoi(string(cmd.Args[4]))
					var expiredAt int64
					expiredAt, e = util.CalcTimeUnix(string(cmd.Args[3]), expired)
					if e != nil {
						conn.WriteError(e.Error() + string(cmd.Args[3]))
						return
					}
					if e != nil {
						conn.WriteError("Cannot Read Expired As Number: " + e.Error())
						return
					}
					e = db.stringIndex.SetNX(cmd.Args[1], cmd.Args[2], expiredAt)
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString("OK")
					return
				} else {
					// 参数数量错误
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "get":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: get")
				if len(cmd.Args) == 2 {
					// get key
					var result string
					result, e = db.stringIndex.Get(cmd.Args[1])
					if errors.Is(logger.KeyIsNotExisted, e) {
						conn.WriteString("nil")
						return
					} else if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString(result)
					return
				} else {
					// 参数数量错误
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "getrange":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: getrange")
				if len(cmd.Args) == 4 {
					// getrange key start end
					var (
						result string
						start  int
						end    int
					)
					start, e = strconv.Atoi(string(cmd.Args[2]))
					if e != nil {
						conn.WriteError("Cannot Read Start As Number: " + e.Error())
						return
					}
					end, e = strconv.Atoi(string(cmd.Args[3]))
					if e != nil {
						conn.WriteError("Cannot Read End As Number: " + e.Error())
						return
					}
					result, e = db.stringIndex.GetRange(cmd.Args[1], start, end)
					if errors.Is(logger.KeyIsNotExisted, e) {
						conn.WriteString("nil")
					} else if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString(result)
					return
				} else {
					// 参数数量错误
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "getset":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: getset")
				if len(cmd.Args) == 3 {
					// getset key value
					var result string
					result, e = db.stringIndex.GetSet(cmd.Args[1], cmd.Args[2])
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString(result)
					return
				} else {
					// 参数数量错误
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "append":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: append")
				if len(cmd.Args) == 3 {
					// append key appendValue
					e = db.stringIndex.Append(cmd.Args[1], cmd.Args[2])
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteInt(len(cmd.Args[2]))
					return
				} else {
					// 参数数量错误
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "del":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: del")
				if len(cmd.Args) == 2 {
					// del key
					e = db.stringIndex.Del(cmd.Args[1])
					if e != nil {
						if errors.Is(logger.KeyIsNotExisted, e) {
							conn.WriteInt(0)
						} else {
							conn.WriteError(e.Error())
						}
						return
					}
					conn.WriteInt(1)
					return
				} else {
					// 参数数量错误
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}

			// hash部分的命令解析
			case "hset":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: hset")
				if len(cmd.Args) == 4 {
					// hset key field value
					e = db.hashIndex.HSet(string(cmd.Args[1]), string(cmd.Args[2]), string(cmd.Args[3]), -1)
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString("OK")
					return
				} else if len(cmd.Args) == 6 {
					// hset key field value ex/px time
					// 设置过期时间
					expired, e = strconv.Atoi(string(cmd.Args[5]))
					var expiredAt int64
					expiredAt, e = util.CalcTimeUnix(string(cmd.Args[4]), expired)
					if e != nil {
						conn.WriteError(e.Error() + string(cmd.Args[4]))
						return
					}
					e = db.hashIndex.HSet(string(cmd.Args[1]), string(cmd.Args[2]), string(cmd.Args[3]), expiredAt)
				} else {
					// 参数数量错误
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "hsetnx":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: hsetnx")
				if len(cmd.Args) == 4 {
					// hset key field value
					e = db.hashIndex.HSetNX(string(cmd.Args[1]), string(cmd.Args[2]), string(cmd.Args[3]), -1)
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString("OK")
					return
				} else if len(cmd.Args) == 6 {
					// hsetnx key field value ex/px time
					// 设置过期时间
					expired, e = strconv.Atoi(string(cmd.Args[5]))
					var expiredAt int64
					expiredAt, e = util.CalcTimeUnix(string(cmd.Args[4]), expired)
					if e != nil {
						conn.WriteError(e.Error() + string(cmd.Args[4]))
						return
					}
					e = db.hashIndex.HSetNX(string(cmd.Args[1]), string(cmd.Args[2]), string(cmd.Args[3]), expiredAt)
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString("OK")
				} else {
					// 参数数量错误
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "hget":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: hget")
				if len(cmd.Args) == 3 {
					// hget key field
					var result string
					result, e = db.hashIndex.HGet(string(cmd.Args[1]), string(cmd.Args[2]))
					if errors.Is(logger.KeyIsNotExisted, e) || errors.Is(logger.FieldIsNotExisted, e) {
						conn.WriteString("nil")
					} else if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString(result)
					return
				} else {
					// 参数数量错误
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "hdel":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: hdel")
				if len(cmd.Args) == 3 {
					// hdel key field
					e = db.hashIndex.HDel(string(cmd.Args[1]), string(cmd.Args[2]), true)
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteInt(1)
					return
				} else if len(cmd.Args) == 2 {
					// hdel key
					e = db.hashIndex.HDel(string(cmd.Args[1]), "", false)
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteInt(1)
					return
				} else {
					// 参数数量错误
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "hlen":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: hlen")
				if len(cmd.Args) == 2 {
					// hlen key
					var result int
					result, e = db.hashIndex.HLen(string(cmd.Args[1]))
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteInt(result)
					return
				} else {
					// 参数数量错误
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "hexists":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: hexists")
				if len(cmd.Args) == 2 {
					// hexists key field
					var result bool
					result, e = db.hashIndex.HExist(string(cmd.Args[1]), string(cmd.Args[2]))
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					if result {
						conn.WriteInt(1)
					} else {
						conn.WriteInt(0)
					}
					return
				} else {
					// 参数数量错误
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "hstrlen":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: hstrlen")
				if len(cmd.Args) == 2 {
					// hstrlen key field
					var result int
					result, e = db.hashIndex.HStrLen(string(cmd.Args[1]), string(cmd.Args[2]))
					if errors.Is(logger.FieldIsNotExisted, e) {
						conn.WriteInt(0)
						return
					} else if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteInt(result)
					return
				} else {
					// 参数数量错误
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}

			// list 部分的命令解析
			case "linsert":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: linsert")
				if len(cmd.Args) == 4 {
					// linsert key index value
					i, e := strconv.Atoi(string(cmd.Args[2]))
					if e != nil {
						conn.WriteError("Cannot Read Index As Number: " + e.Error())
						return
					}
					e = db.listIndex.LInsert(cmd.Args[1], i, cmd.Args[3], -1)
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString("OK")
					return
				} else if len(cmd.Args) == 6 {
					// linsert key index value ex/px time
					i, e := strconv.Atoi(string(cmd.Args[2]))
					if e != nil {
						conn.WriteError("Cannot Read Index As Number: " + e.Error())
						return
					}
					expired, e = strconv.Atoi(string(cmd.Args[5]))
					if e != nil {
						conn.WriteError("Cannot Read Expired As Number: " + e.Error())
						return
					}
					var expiredAt int64
					expiredAt, e = util.CalcTimeUnix(string(cmd.Args[4]), expired)
					e = db.listIndex.LInsert(cmd.Args[1], i, cmd.Args[3], expiredAt)
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString("OK")
					return
				} else {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "lpop":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: lpop")
				if len(cmd.Args) == 2 {
					// lpop key
					v, e := db.listIndex.LPop(cmd.Args[1])
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString(string(v))
					return
				} else {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "lpush":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: lpush")
				if len(cmd.Args) == 3 {
					// lpush key value
					e = db.listIndex.LPush(cmd.Args[1], -1, cmd.Args[2])
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString("OK")
					return
				} else if len(cmd.Args) == 5 {
					// lpush key value ex/px time
					expired, e = strconv.Atoi(string(cmd.Args[4]))
					if e != nil {
						conn.WriteError("Cannot Read Expired As Number: " + e.Error())
						return
					}
					var expiredAt int64
					expiredAt, e = util.CalcTimeUnix(string(cmd.Args[3]), expired)
					e = db.listIndex.LPush(cmd.Args[1], expiredAt, cmd.Args[3])
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString("OK")
					return
				} else {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "lset":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: lset")
				if len(cmd.Args) == 4 {
					// lset key index value
					i, e := strconv.Atoi(string(cmd.Args[2]))
					if e != nil {
						conn.WriteError("Cannot Read Index As Number: " + e.Error())
						return
					}
					e = db.listIndex.LSet(cmd.Args[1], i, cmd.Args[2])
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString("OK")
					return
				} else {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "lrem":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: lrem")
				if len(cmd.Args) == 4 {
					// lrem key index value
					i, e := strconv.Atoi(string(cmd.Args[2]))
					if e != nil {
						conn.WriteError("Cannot Read Index As Number: " + e.Error())
						return
					}
					e = db.listIndex.LRem(cmd.Args[1], i, cmd.Args[2])
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString("OK")
					return
				} else {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "llen":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: " + string(cmd.Args[0]))
				if len(cmd.Args) == 2 {
					// llen key
					result, e := db.listIndex.LLen(cmd.Args[1])
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteInt(result)
					return
				} else {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "lindex":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: " + string(cmd.Args[0]))
				if len(cmd.Args) == 3 {
					// lindex key index
					i, e := strconv.Atoi(string(cmd.Args[2]))
					if e != nil {
						conn.WriteError("Cannot Read Index As Number: " + e.Error())
						return
					}
					result, e := db.listIndex.LIndex(cmd.Args[1], i)
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString(string(result))
					return
				} else {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "lrange":
				logger.GenerateInfoLog(conn.RemoteAddr() + ": Query: " + string(cmd.Args[0]))
				if len(cmd.Args) == 4 {
					// lrange key start end
					start, e := strconv.Atoi(string(cmd.Args[2]))
					if e != nil {
						conn.WriteError("Cannot Read Start As Number: " + e.Error())
						return
					}
					end, e := strconv.Atoi(string(cmd.Args[3]))
					if e != nil {
						conn.WriteError("Cannot Read End As Number: " + e.Error())
						return
					}
					result, e := db.listIndex.LRange(cmd.Args[1], start, end)
					if e != nil {
						conn.WriteError(e.Error())
						return
					}
					conn.WriteString(util.TurnByteArray2ToString(result))
					return
				} else {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			}
		},
		func(conn redcon.Conn) bool {
			logger.GenerateInfoLog("DataBase Connection Accept: " + conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			logger.GenerateInfoLog("DataBase Connection Closed: " + conn.RemoteAddr())
			return
		},
	)

	return nil
}

func (db *MisakaDataBase) StartServe() error {
	logger.GenerateInfoLog("Server start Listen And Serve!")
	return db.server.ListenAndServe()
	// 翻源码可知：
	// ListenAndServe -> ListenServeAndSignal -> serve -> 如果有tcp连接 -> go handle
	// 所以ListenServeAndSignal是阻塞线程监听的
}
