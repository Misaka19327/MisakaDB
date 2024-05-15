package src

import (
	"MisakaDB/src/index"
	"MisakaDB/src/logger"
	"MisakaDB/src/storage"
	"errors"
	"github.com/tidwall/redcon"
	"strings"
)

// 以下为可选配置项
const (
	MisakaDataBaseFolderPath = "D:\\MisakaDBTest"
	RecordFileMaxSize        = 65536
	RecordFileIOMode         = storage.TraditionalIOFile
	MisakaServerAddr         = ":6380"
)

type MisakaDataBase struct {
	server *redcon.Server

	hashIndex *index.HashIndex
}

func Init() (*MisakaDataBase, error) {
	database := &MisakaDataBase{}

	// 读取文件
	activeFiles, archiveFiles, e := storage.RecordFilesInit(MisakaDataBaseFolderPath, RecordFileMaxSize)
	if e != nil {
		return nil, e
	}

	// 开始构建索引
	for key, value := range activeFiles {
		if key == storage.Hash {
			database.hashIndex, e = index.BuildHashIndex(value, archiveFiles[storage.Hash], RecordFileIOMode, MisakaDataBaseFolderPath, RecordFileMaxSize)
			if e != nil {
				return nil, e
			}
		}
	}

	// 初始化服务器
	e = database.ServerInit()
	if e != nil {
		return nil, e
	}

	return database, nil
}

func (db *MisakaDataBase) Destroy() error {

	// 关闭服务器
	e := db.server.Close()
	if e != nil {
		return e
	}

	// 关闭索引
	e = db.hashIndex.CloseIndex()
	if e != nil {
		return e
	}
	return nil
}

func (db *MisakaDataBase) ServerInit() error {
	// redcon是多线程的 而且应该是线程安全的

	// 创建一个Server需要三个回调函数：
	// 1 通过连接接收请求时调用的函数
	// 2 接受连接时调用的函数
	// 3 断开连接时调用的函数
	var e error
	db.server = redcon.NewServer(MisakaServerAddr,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				// 命令不能识别
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				e = conn.Close()
				if e != nil {
					// attention logger
				}
			//case "set":
			//	if len(cmd.Args) != 3 {
			//		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			//		return
			//	} else  {
			//
			//	}

			// hash部分的命令解析
			case "hset":
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
					// 设置过期时间
					// todo
				} else {
					// 参数数量错误
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "hsetnx":
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
					// 设置过期时间
					// todo
				} else {
					// 参数数量错误
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
			case "hget":
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
			}
		},
		func(conn redcon.Conn) bool {
			// attention logger
			return true
		},
		func(conn redcon.Conn, err error) {
			// attention logger
		},
	)

	return nil
}

func (db *MisakaDataBase) StartServe() error {

	return db.server.ListenAndServe()
	// 翻源码可知：
	// ListenAndServe -> ListenServeAndSignal -> serve -> 如果有tcp连接 -> go handle
	// 所以ListenServeAndSignal是阻塞线程监听的]
}
