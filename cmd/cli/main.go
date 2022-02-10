package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/gomodule/redigo/redis"
	"github.com/peterh/liner"
)

var commandList = [][]string{
	{"SET", "key value", "STRING"},
	{"GET", "key", "STRING"},
	{"SETNX", "key seconds value", "STRING"},
	{"SETEX", "key value", "STRING"},
	{"GETSET", "key value", "STRING"},
	{"MSET", "[key value...]", "STRING"},
	{"MGET", "[key...]", "STRING"},
	{"APPEND", "key value", "STRING"},
	{"STREXISTS", "key", "STRING"},
	{"REMOVE", "key", "STRING"},
	{"EXPIRE", "key seconds", "STRING"},
	{"PERSIST", "key", "STRING"},
	{"TTL", "key", "STRING"},

	{"LPUSH", "key value [value...]", "LIST"},
	{"RPUSH", "key value [value...]", "LIST"},
	{"LPOP", "key", "LIST"},
	{"RPOP", "key", "LIST"},
	{"LINDEX", "key index", "LIST"},
	{"LREM", "key value count", "LIST"},
	{"LINSERT", "key BEFORE|AFTER pivot element", "LIST"},
	{"LSET", "key index value", "LIST"},
	{"LTRIM", "key start end", "LIST"},
	{"LRANGE", "key start end", "LIST"},
	{"LLEN", "key", "LIST"},
	{"LKEYEXISTS", "key", "LIST"},
	{"LVALEXISTS", "key value", "LIST"},
	{"LClear", "key", "LIST"},
	{"LExpire", "key seconds", "LIST"},
	{"LTTL", "key", "LIST"},

	{"HSET", "key field value", "HASH"},
	{"HSETNX", "key field value", "HASH"},
	{"HGET", "key field", "HASH"},
	{"HMSET", "[key field...]", "HASH"},
	{"HMGET", "[key...]", "HASH"},
	{"HGETALL", "key", "HASH"},
	{"HDEL", "key field [field...]", "HASH"},
	{"HKEYEXISTS", "key", "HASH"},
	{"HEXISTS", "key field", "HASH"},
	{"HLEN", "key", "HASH"},
	{"HKEYS", "key", "HASH"},
	{"HVALS", "key", "HASH"},
	{"HCLEAR", "key", "HASH"},
	{"HEXPIRE", "key seconds", "HASH"},
	{"HTTL", "key", "HASH"},

	{"SADD", "key members [members...]", "SET"},
	{"SPOP", "key count", "SET"},
	{"SISMEMBER", "key member", "SET"},
	{"SRANDMEMBER", "key count", "SET"},
	{"SREM", "key members [members...]", "SET"},
	{"SMOVE", "src dst member", "SET"},
	{"SCARD", "key", "key", "SET"},
	{"SMEMBERS", "key", "SET"},
	{"SUNION", "key [key...]", "SET"},
	{"SDIFF", "key [key...]", "SET"},
	{"SKEYEXISTS", "key", "SET"},
	{"SCLEAR", "key", "SET"},
	{"SEXPIRE", "key seconds", "SET"},
	{"STTL", "key", "SET"},

	{"ZADD", "key score member", "ZSET"},
	{"ZSCORE", "key member", "ZSET"},
	{"ZCARD", "key", "ZSET"},
	{"ZRANK", "key member", "ZSET"},
	{"ZREVRANK", "key member", "ZSET"},
	{"ZINCRBY", "key increment member", "ZSET"},
	{"ZRANGE", "key start stop", "ZSET"},
	{"ZREVRANGE", "key start stop", "ZSET"},
	{"ZREM", "key member", "ZSET"},
	{"ZGETBYRANK", "key rank", "ZSET"},
	{"ZREVGETBYRANK", "key rank", "ZSET"},
	{"ZSCORERANGE", "key min max", "ZSET"},
	{"ZREVSCORERANGE", "key max min", "ZSET"},
	{"ZKEYEXISTS", "key", "ZSET"},
	{"ZCLEAR", "key", "ZSET"},
	{"ZEXPIRE", "key", "ZSET"},
	{"ZTTL", "key", "ZSET"},

	{"MULTI", "Transaction start", "TRANSACTION"},
	{"EXEC", "Transaction end", "TRANSACTION"},
}

var host = flag.String("h", "127.0.0.1", "the rosedb server host,default 127.0.0.1")
var port = flag.Int("p", 5200, "the rosedb server port,default 5200")

const cmdHistoryPath = "/tmp/rose-cli"

func main() {
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", *host, *port)
	conn, err := redis.Dial("tcp", addr)
	if err != nil {
		log.Println("tcp dial err:", err)
		return
	}

	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)
	line.SetCompleter(func(li string) (res []string) {
		for _, c := range commandList {
			if strings.HasPrefix(c[0], strings.ToUpper(li)) {
				res = append(res, strings.ToLower(c[0]))
			}
		}
		return
	})

	if f, err := os.Open(cmdHistoryPath); err == nil {
		line.ReadHistory(f)
		f.Close()
	}
	defer func() {
		if f, err := os.Create(cmdHistoryPath); err != nil {
			fmt.Printf("writing cmd history err:%v\n", err)
		} else {
			line.WriteHistory(f)
			f.Close()
		}
	}()

	commandSet := map[string]bool{}
	for _, cmd := range commandList {
		commandSet[strings.ToLower(cmd[0])] = true
	}
}
