package main

import (
	"log"
	"runtime"
	"sync"

	"github.com/tidwall/redcon"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

// Wrap operations in a struct that can be passed over a channel to a
// worker goroutine.
type lmdbop struct {
	op  lmdb.TxnOp
	res chan<- error
}

type Handler struct {
	envMux sync.RWMutex
	env    *lmdb.Env
	dbi    lmdb.DBI
	worker chan *lmdbop
}

func NewHandler(path string) (*Handler, error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		return nil, err
	}
	err = env.SetMapSize(5 * 1 << 30)
	if err != nil {
		return nil, err
	}
	err = env.SetMaxDBs(1)
	if err != nil {
		return nil, err
	}
	err = env.Open(path, lmdb.NoSubdir, 0644)
	if err != nil {
		return nil, err
	}
	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenRoot(0)
		return err
	})
	if err != nil {
		env.Close()
		return nil, err
	}

	worker := make(chan *lmdbop)

	// Start issuing update operations in a goroutine in which we know
	// runtime.LockOSThread can be called and we can safely issue transactions.
	go func() {
		runtime.LockOSThread()
		defer runtime.LockOSThread()

		// Perform each operation as we get to it.  Because this goroutine is
		// already locked to a thread, env.UpdateLocked is called to avoid
		// premature unlocking of the goroutine from its thread.
		for op := range worker {
			op.res <- env.UpdateLocked(op.op)
		}
	}()

	return &Handler{
		env:    env,
		dbi:    dbi,
		worker: worker,
	}, nil
}

func (h *Handler) Close() error {
	return h.env.Close()
}

func (h *Handler) view(op lmdb.TxnOp) error {
	h.envMux.RLock()
	err := h.env.View(op)
	h.envMux.RUnlock()
	return err
}

func (h *Handler) update(op lmdb.TxnOp) error {
	res := make(chan error)
	h.envMux.Lock()
	h.worker <- &lmdbop{op, res}
	h.envMux.Unlock()
	return <-res
}

func (h *Handler) detach(conn redcon.Conn, cmd redcon.Command) {
	detachedConn := conn.Detach()
	log.Printf("connection has been detached")
	go func(c redcon.DetachedConn) {
		defer c.Close()

		c.WriteString("OK")
		c.Flush()
	}(detachedConn)
}

func (h *Handler) ping(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("PONG")
}

func (h *Handler) quit(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("OK")
	conn.Close()
}

func (h *Handler) set(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	_ = h.update(func(txn *lmdb.Txn) error {
		err := txn.Put(h.dbi, cmd.Args[1], cmd.Args[2], 0)
		if err != nil {
			conn.WriteNull()
		} else {
			conn.WriteString("OK")
		}
		return err
	})
}

func (h *Handler) get(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	_ = h.view(func(txn *lmdb.Txn) error {
		val, err := txn.Get(h.dbi, cmd.Args[1])
		if err != nil {
			conn.WriteNull()
		} else {
			conn.WriteBulk(val)
		}
		return err
	})
}

func (h *Handler) setnx(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	err := h.view(func(txn *lmdb.Txn) error {
		_, err := txn.Get(h.dbi, cmd.Args[1])
		return err
	})
	if err == nil {
		conn.WriteInt(0)
		return
	}

	err = h.update(func(txn *lmdb.Txn) error {
		err := txn.Put(h.dbi, cmd.Args[1], cmd.Args[2], lmdb.NoOverwrite)
		return err
	})
	if err == nil {
		conn.WriteInt(1)
		return
	}
}

func (h *Handler) delete(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	_ = h.update(func(txn *lmdb.Txn) error {
		err := txn.Del(h.dbi, cmd.Args[1], nil)
		if err != nil {
			conn.WriteInt(0)
		} else {
			conn.WriteInt(1)
		}
		return err
	})
}
