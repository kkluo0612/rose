package cmd

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/roseduan/rosedb"
	"github.com/tidwall/redcon"
)

type ExecCmdFunc func(*rosedb.RoseDB, []string) (interface{}, error)

var ExecCmd = make(map[string]ExecCmdFunc)

var (
	nestedMultiErr  = errors.New("ERR MULTI calls can not be nested")
	withoutMultiErr = errors.New("ERR EXEC without MULTI")
	execAbortErr    = errors.New("EXECABORT Transaction discarded because of previous error")
)

func addExecCommand(cmd string, cmdFunc ExecCmdFunc) {
	ExecCmd[strings.ToLower(cmd)] = cmdFunc
}

type Server struct {
	server  *redcon.Server
	db      *rosedb.RoseDB
	closed  bool
	mu      sync.Mutex
	TxnList sync.Map
}

type TxnList struct {
	cmdArgs [][]string
	err     error
}

func NewServer(config rosedb.Config) (*Server, error) {
	db, err := rosedb.Open(config)
	if err != nil {
		return nil, err
	}
	return &Server{db: db}, nil
}

func NewServerUseDbPtr(db *rosedb.RoseDB) *Server {
	return &Server{db: db}
}

func (s *Server) Listen(addr string) {
	svr := redcon.NewServerNetwork("tcp", addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			s.handleCmd(conn, cmd)
		},
		func(conn redcon.Conn) bool {
			return true
		},
		func(conn redcon.Conn, err error) {
			s.TxnList.Delete(conn.RemoteAddr())
		},
	)

	s.server = svr
	log.Println("rosedb is running,ready to accept connections.")
	if err := svr.ListenAndServe(); err != nil {
		log.Printf("Listen and server occurs error: %+v\n", err)
	}
}

func (s *Server) Stop() {
	if s.closed {
		return
	}
	s.mu.Lock()
	s.closed = true
	if err := s.server.Close(); err != nil {
		log.Printf("Close redcon err: %+v\n", err)
	}
	if err := s.db.Close(); err != nil {
		log.Printf("close rosedb err: %+v\n", err)
	}
	s.mu.Unlock()
}

func (s *Server) handleCmd(conn redcon.Conn, cmd redcon.Command) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic when handle the cmd: %+v\n", r)
		}
	}()

	var reply interface{}
	var err error
	command := strings.ToLower(string(cmd.Args[0]))
	if command == "mutli" {
		if _, ok := s.TxnList.Load(conn.RemoteAddr()); !ok {
			var txnList TxnList
			s.TxnList.Store(conn.RemoteAddr(), &txnList)
			reply = "OK"
		} else {
			err = nestedMultiErr
		}
	} else if command == "exec" {
		if value, ok := s.TxnList.Load(conn.RemoteAddr()); ok {
			s.TxnList.Delete(conn.RemoteAddr())
			txnList := value.(*TxnList)
			if txnList.err != nil {
				err = execAbortErr
			} else {
				if len(txnList.cmdArgs) == 0 {
					reply = "(empty list or set)"
				} else {
					reply, err = txn(s.db, txnList.cmdArgs)
				}
			}
		} else {
			err = withoutMultiErr
		}
	} else {
		if value, ok := s.TxnList.Load(conn.RemoteAddr()); ok {
			txnList := value.(*TxnList)
			_, exist := ExecCmd[command]
			if !exist {
				txnList.err = fmt.Errorf("ERR unknow command '%s'", command)
				conn.WriteError(txnList.err.Error())
				return
			}
			txnList.cmdArgs = append(txnList.cmdArgs, parseTxnArgs(cmd.Args))
			reply = "QUEUED"
		} else {
			exec, exist := ExecCmd[command]
			if !exist {
				conn.WriteError(fmt.Sprintf("ERR unknow command '%s'", command))
				return
			}
			args := parseArgs(cmd.Args)
			reply, err = exec(s.db, args)
		}
	}
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteAny(reply)
}

func parseArgs(cmdArgs [][]byte) []string {
	args := make([]string, 0, len(cmdArgs)-1)
	for i, bytes := range cmdArgs {
		if i == 0 {
			continue
		}
		args = append(args, string(bytes))
	}
	return args
}

func parseTxnArgs(cmdArgs [][]byte) []string {
	args := make([]string, 0, len(cmdArgs))
	for _, bytes := range cmdArgs {
		args = append(args, string(bytes))
	}
	return args
}
