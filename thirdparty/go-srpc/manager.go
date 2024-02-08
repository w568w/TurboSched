package srpc

import (
	"context"
	"errors"
	"github.com/smallnest/rpcx/server"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

var errNoSuchSession = errors.New("srpc-server: no such session")

type StreamManager struct {
	id       uint64
	sessions sync.Map
}
type IStreamManager interface {
	Poll(ctx context.Context, sid uint64, reply *[]*StreamEvent) error
	Cancel(ctx context.Context, sid uint64, loaded *bool) error
	SoftCancel(ctx context.Context, sid uint64, loaded *bool) error
}

var Manager = StreamManager{id: 0}

func S(f func() error, sess *Session, cfg *SessionConfig) error {

	sid := atomic.AddUint64(&Manager.id, 1)
	sess.initSession(sid, cfg)

	Manager.sessions.Store(sid, sess)

	go func() {
		defer func() {
			if p := recover(); p != nil {
				serverLogFunc("%+v\n", p)
				sess.pushPanic(&panicInfo{
					Data:  p,
					Stack: debug.Stack(),
				})
			}
			sess.waitFlush(sess.cfg.KeepAlive)
			Manager.sessions.Delete(sid)
		}()
		sess.mamo.Loop()
		defer close(sess.doneCh)
		err := f()
		if err == nil {
			sess.pushDone()
		} else {
			sess.pushError(err)
		}
	}()

	return nil
}

func (m *StreamManager) Poll(sid uint64, reply *[]*StreamEvent) error {
	v, loaded := Manager.sessions.Load(sid)
	if !loaded {
		return errNoSuchSession
	}
	sess := v.(*Session)
	sess.mamo.Acquire()
	defer sess.mamo.Release()
	*reply = sess.flush()
	return nil
}

func (m *StreamManager) Cancel(sid uint64, loaded *bool) error {
	var sess any
	sess, *loaded = Manager.sessions.LoadAndDelete(sid)
	if !*loaded {
		return nil
	}
	sess.(*Session).cancel()

	return nil
}

func (m *StreamManager) SoftCancel(sid uint64, loaded *bool) error {
	var sess any
	sess, *loaded = Manager.sessions.Load(sid)
	if !*loaded {
		return nil
	}
	sess.(*Session).cancel()

	return nil
}

func RegisterNameWithStream(server *server.Server, name string, rcvr IStreamManager, metadata string) error {
	return server.RegisterName(name, rcvr, metadata)
}
