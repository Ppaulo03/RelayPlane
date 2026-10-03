//go:build integration && chaos

package systemtest

import (
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// netProxy is a TCP proxy in front of a real service that can add latency/jitter to every chunk of traffic
// and reset all connections on demand (what a flaky network, a failing load balancer or a NAT timeout does).
type netProxy struct {
	ln      net.Listener
	target  string
	latency atomic.Int64 // base delay per chunk, ns
	jitter  atomic.Int64 // extra uniform random delay per chunk, ns
	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	closed  atomic.Bool
}

func newNetProxy(t *testing.T, target string) *netProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &netProxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	go p.accept()
	t.Cleanup(p.close)
	return p
}

func (p *netProxy) Addr() string { return p.ln.Addr().String() }

// Set configures the delay applied to each forwarded chunk.
func (p *netProxy) Set(latency, jitter time.Duration) {
	p.latency.Store(int64(latency))
	p.jitter.Store(int64(jitter))
}

func (p *netProxy) delay() {
	d := time.Duration(p.latency.Load())
	if j := p.jitter.Load(); j > 0 {
		d += time.Duration(rand.Int63n(j))
	}
	if d > 0 {
		time.Sleep(d)
	}
}

func (p *netProxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		up, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		p.mu.Lock()
		p.conns[c], p.conns[up] = struct{}{}, struct{}{}
		p.mu.Unlock()
		pipe := func(dst, src net.Conn) {
			buf := make([]byte, 32*1024)
			for {
				n, err := src.Read(buf)
				if n > 0 {
					p.delay()
					if _, werr := dst.Write(buf[:n]); werr != nil {
						break
					}
				}
				if err != nil {
					break
				}
			}
			_ = dst.Close()
			_ = src.Close()
		}
		go pipe(up, c)
		go pipe(c, up)
	}
}

// Sever resets every open connection; new ones are accepted normally.
func (p *netProxy) Sever() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetLinger(0) // RST instead of FIN
		}
		_ = c.Close()
		delete(p.conns, c)
	}
}

func (p *netProxy) close() {
	if p.closed.Swap(true) {
		return
	}
	_ = p.ln.Close()
	p.Sever()
}
