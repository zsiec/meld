package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync/atomic"

	"github.com/zsiec/meld"
)

// monoFont is a monospace TrueType face used to burn the per-frame timestamp into the
// captured video. Menlo ships on every macOS; freetype/drawtext read the .ttc directly.
const monoFont = "/System/Library/Fonts/Menlo.ttc"

// drawtextFilter returns an ffmpeg -vf value that overlays each frame's presentation
// timestamp (HH:MM:SS.mmm) in the corner, or "" if the font is unavailable. In the
// recursive "video tunnel" (camera pointed at the display) every nested ring shows an
// older timestamp, so the delta between adjacent rings IS one glass-to-glass latency —
// the showcase measures itself. The colon in %{pts:hms} is escaped so ffmpeg's filter
// parser does not read it as an option separator.
func drawtextFilter() string {
	if _, err := os.Stat(monoFont); err != nil {
		return ""
	}
	return "drawtext=fontfile=" + monoFont +
		":text='%{pts\\:hms}':x=24:y=24:fontsize=48:fontcolor=yellow:" +
		"box=1:boxcolor=black@0.6:boxborderw=12"
}

// captureCommand builds the ffmpeg process that produces the low-latency H.264 elementary
// stream on stdout (pipe:1). The source is the macOS camera (avfoundation) or, for headless
// runs, a synthetic lavfi pattern. Encoding is tuned for minimum latency: ultrafast preset,
// zerolatency tune (which also disables B-frames), a 1-second-ish keyframe interval, and a
// 1-frame VBV buffer so the encoder never holds frames back.
func captureCommand(ctx context.Context, o *options) *exec.Cmd {
	args := []string{"-hide_banner", "-loglevel", o.ffLogLevel()}
	if o.input == "test" {
		// -re paces the synthetic source at wall-clock rate so it behaves like the
		// realtime camera (and is watchable when used as the no-camera fallback).
		args = append(args, "-re", "-f", "lavfi", "-i",
			fmt.Sprintf("testsrc2=size=%s:rate=%d", o.size, o.fps))
	} else {
		args = append(args,
			"-f", "avfoundation",
			"-framerate", strconv.Itoa(o.fps),
			"-video_size", o.size,
			"-i", o.device)
	}
	if vf := drawtextFilter(); vf != "" {
		args = append(args, "-vf", vf)
	}
	args = append(args,
		"-c:v", "libx264",
		"-preset", "ultrafast",
		"-tune", "zerolatency",
		"-g", strconv.Itoa(o.gop),
		"-bf", "0",
		"-pix_fmt", "yuv420p",
		"-b:v", o.bitrate, "-maxrate", o.bitrate, "-bufsize", o.bitrate,
		"-f", "h264", "pipe:1")
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Stderr = os.Stderr
	return cmd
}

// playCommand builds the ffplay process that decodes the recovered H.264 from stdin
// (pipe:0) and shows it, with every buffering and probing knob set for minimum display
// latency so the window reflects what Meld just delivered.
func playCommand(ctx context.Context, o *options) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "ffplay",
		"-hide_banner", "-loglevel", o.ffLogLevel(),
		"-fflags", "nobuffer",
		"-flags", "low_delay",
		"-framedrop",
		"-probesize", "32",
		"-analyzeduration", "0",
		"-window_title", "Meld latency mirror — point the camera at this window",
		"-f", "h264", "-i", "pipe:0")
	cmd.Stderr = os.Stderr
	return cmd
}

// pumpToSender copies the H.264 elementary stream from the encoder into the Meld sender,
// one chunk per Write (each Write is delivered to the receiver byte-exact and in order, so
// concatenating the receiver's reads reconstructs the stream regardless of chunk
// boundaries). max is Config.MaxChunk. It returns when the source reaches EOF (the encoder
// exited) or a Write fails (the sender closed).
func pumpToSender(tx *meld.Sender, src io.Reader, max int) error {
	buf := make([]byte, max)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			chunk := make([]byte, n) // defensive copy; tx must not see a reused buffer
			copy(chunk, buf[:n])
			if _, werr := tx.Write(chunk); werr != nil {
				return werr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// pumpFromReceiver copies in-order delivered chunks from the Meld receiver into the sink
// (ffplay's stdin, or a file). max must be at least Config.SymbolSize. It returns when the
// receiver is closed or the sink fails.
func pumpFromReceiver(rx *meld.Receiver, dst io.Writer, max int) error {
	buf := make([]byte, max)
	for {
		n, err := rx.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
	}
}

// lossyProxy is an in-process UDP relay between the sender and receiver that drops
// sender->receiver datagrams with a fixed probability and relays receiver->sender feedback
// losslessly. It lets the loopback mirror double as a survivability demo: crank -loss up
// and watch Meld's coding keep the tunnel flowing where raw UDP would freeze. The forwarding
// logic mirrors the lossyProxy in the repo's e2e_test.go.
type lossyProxy struct {
	conn    *net.UDPConn
	rxAddr  *net.UDPAddr
	sender  *net.UDPAddr
	rng     *rand.Rand
	lossPct float64
	drop    int64
	fwd     int64
}

// newLossyProxy binds an ephemeral loopback socket the sender dials, forwarding toward the
// real receiver at rxAddr while dropping the given fraction of forward datagrams. seed makes
// the drop pattern reproducible.
func newLossyProxy(rxAddr string, lossPct float64, seed int64) (*lossyProxy, error) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return nil, err
	}
	raddr, err := net.ResolveUDPAddr("udp", rxAddr)
	if err != nil {
		_ = pc.Close()
		return nil, err
	}
	p := &lossyProxy{conn: pc, rxAddr: raddr, rng: rand.New(rand.NewSource(seed)), lossPct: lossPct}
	go p.run()
	return p, nil
}

// addr is the loopback address the sender should dial.
func (p *lossyProxy) addr() string     { return p.conn.LocalAddr().String() }
func (p *lossyProxy) dropped() int64   { return atomic.LoadInt64(&p.drop) }
func (p *lossyProxy) forwarded() int64 { return atomic.LoadInt64(&p.fwd) }

// Close stops the relay.
func (p *lossyProxy) Close() error { return p.conn.Close() }

func (p *lossyProxy) run() {
	buf := make([]byte, 2048)
	for {
		n, src, err := p.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if src.Port == p.rxAddr.Port && src.IP.Equal(p.rxAddr.IP) {
			if p.sender != nil { // feedback receiver -> sender, lossless
				_, _ = p.conn.WriteToUDP(buf[:n], p.sender)
			}
			continue
		}
		p.sender = src // symbol sender -> receiver
		if p.rng.Float64() < p.lossPct {
			atomic.AddInt64(&p.drop, 1)
			continue
		}
		atomic.AddInt64(&p.fwd, 1)
		_, _ = p.conn.WriteToUDP(buf[:n], p.rxAddr)
	}
}
