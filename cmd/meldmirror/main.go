// Command meldmirror is the live latency-mirror showcase for the Meld transport.
//
// It captures the webcam, encodes a low-latency H.264 stream with ffmpeg, streams it
// through a Meld Sender/Receiver pair over UDP, and shows the recovered video with
// ffplay. Meld is the only thing between the two pixel pipelines: capture -> encode ->
// MELD over the network -> decode -> display.
//
// Point the camera at the display and you get the recursive "video tunnel": each nested
// ring is one more trip through the whole glass-to-glass pipeline, so the depth of the
// tunnel visualizes the end-to-end latency. A timestamp burned into each frame
// (HH:MM:SS.mmm) lets you read the latency off a single photo — the delta between any two
// adjacent rings is one glass-to-glass round.
//
// The default "mirror" mode runs both endpoints in one process over loopback. Use -mode
// send / -mode recv to split it across two machines, and -loss to route the loopback
// through an in-process lossy relay so the demo doubles as a survivability showcase.
//
// Requires ffmpeg and ffplay on PATH (Homebrew: `brew install ffmpeg`). It shells out to
// them for capture/codec/display only; Meld itself adds no dependencies.
//
// Examples:
//
//	meldmirror                          # loopback mirror, FaceTime camera, ffplay window
//	meldmirror -loss 0.2                # same, but drop 20% of uplink datagrams
//	meldmirror -mode recv -listen :5000 # on the display host
//	meldmirror -mode send -connect host:5000 -device 0   # on the camera host
//	meldmirror -input test -output /tmp/out.h264 -duration 5s   # headless self-test
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/zsiec/meld"
)

// options holds the parsed command-line configuration for one meldmirror run.
type options struct {
	mode       string
	connect    string
	listen     string
	device     string
	input      string
	output     string
	size       string
	bitrate    string
	fps        int
	gop        int
	buffer     time.Duration
	loss       float64
	redundancy float64
	sliding    bool
	pace       bool
	duration   time.Duration
	verbose    bool
}

func parseFlags() *options {
	o := &options{}
	flag.StringVar(&o.mode, "mode", "mirror", "mirror (both ends, loopback) | send | recv")
	flag.StringVar(&o.connect, "connect", "", "send mode: receiver address host:port")
	flag.StringVar(&o.listen, "listen", "127.0.0.1:0", "recv mode: bind address (:port for all interfaces)")
	flag.StringVar(&o.device, "device", "0", "avfoundation video device index/name")
	flag.StringVar(&o.input, "input", "camera", "camera (avfoundation) | test (synthetic, headless)")
	flag.StringVar(&o.output, "output", "play", "play (ffplay window) | a file path (write raw H.264)")
	flag.StringVar(&o.size, "size", "1280x720", "capture resolution WxH")
	flag.StringVar(&o.bitrate, "bitrate", "3M", "H.264 target/cap bitrate")
	flag.IntVar(&o.fps, "fps", 30, "capture framerate")
	flag.IntVar(&o.gop, "gop", 30, "keyframe interval in frames")
	flag.DurationVar(&o.buffer, "buffer", 50*time.Millisecond, "Meld playout/deadline budget — must exceed the real end-to-end transport latency (code→wire→decode); push it lower on a fast/idle path, raise it on a busy machine or long link")
	flag.Float64Var(&o.loss, "loss", 0, "mirror mode: fraction of uplink datagrams to drop (0..1)")
	flag.Float64Var(&o.redundancy, "redundancy", -1, "Meld proactive code-rate floor (<0 = default)")
	flag.BoolVar(&o.sliding, "sliding", true, "use the band-form sliding-window coder (default main profile; set false for generation fallback)")
	flag.BoolVar(&o.pace, "pace", true, "pace coded datagrams onto the wire across the budget (off = send each emit immediately, lowest latency)")
	flag.DurationVar(&o.duration, "duration", 0, "stop after this long (0 = run until Ctrl-C)")
	flag.BoolVar(&o.verbose, "verbose", false, "show ffmpeg/ffplay info logging")
	flag.Parse()
	return o
}

// ffLogLevel maps -verbose to an ffmpeg/ffplay -loglevel.
func (o *options) ffLogLevel() string {
	if o.verbose {
		return "info"
	}
	return "error"
}

// meldConfig builds the Meld Config from the flags, starting from the library defaults.
func (o *options) meldConfig() meld.Config {
	cfg := meld.DefaultConfig()
	cfg.Flow = 1
	cfg.BufferMicros = o.buffer.Microseconds()
	cfg.Sliding = o.sliding
	cfg.Pace = o.pace
	if o.redundancy >= 0 {
		cfg.Redundancy = o.redundancy
	}
	return cfg
}

func main() {
	o := parseFlags()
	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "meldmirror:", err)
		os.Exit(1)
	}
}

func run(o *options) error {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return fmt.Errorf("ffmpeg not found on PATH (brew install ffmpeg): %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if o.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.duration)
		defer cancel()
	}

	switch o.mode {
	case "mirror":
		return runMirror(ctx, o)
	case "send":
		return runSend(ctx, o)
	case "recv":
		return runRecv(ctx, o)
	default:
		return fmt.Errorf("unknown -mode %q (want mirror, send, or recv)", o.mode)
	}
}

// runMirror runs both endpoints in one process over loopback: capture -> Meld -> display,
// optionally through a lossy relay. This is the self-contained desktop showcase.
func runMirror(ctx context.Context, o *options) error {
	cfg := o.meldConfig()

	rx, err := meld.NewReceiver("127.0.0.1:0", cfg)
	if err != nil {
		return fmt.Errorf("receiver: %w", err)
	}
	defer func() { _ = rx.Close() }()

	target := rx.LocalAddr()
	var proxy *lossyProxy
	if o.loss > 0 {
		proxy, err = newLossyProxy(rx.LocalAddr(), o.loss, 1)
		if err != nil {
			return fmt.Errorf("loss relay: %w", err)
		}
		defer func() { _ = proxy.Close() }()
		target = proxy.addr()
	}

	tx, err := meld.NewSender(target, cfg)
	if err != nil {
		return fmt.Errorf("sender: %w", err)
	}
	defer func() { _ = tx.Close() }()

	sink, closeSink, sinkCmd, err := openSink(ctx, o)
	if err != nil {
		return err
	}
	defer func() { _ = closeSink() }()

	cap := captureCommand(ctx, o)
	stdout, err := cap.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cap.Start(); err != nil {
		return fmt.Errorf("start capture: %w", err)
	}

	fmt.Fprintf(os.Stderr, "meldmirror: %s -> Meld (%s, %.0fms buffer", sourceLabel(o), target, o.buffer.Seconds()*1000)
	if o.loss > 0 {
		fmt.Fprintf(os.Stderr, ", %.0f%% uplink loss", o.loss*100)
	}
	fmt.Fprintf(os.Stderr, ") -> %s\n", sinkLabel(o))
	if o.output == "play" && o.input == "camera" {
		fmt.Fprintln(os.Stderr, "meldmirror: point the camera at the ffplay window for the recursive latency tunnel. Ctrl-C to stop.")
	}

	var wg sync.WaitGroup
	pumpErr := make(chan error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); pumpErr <- pumpFromReceiver(rx, sink, cfg.SymbolSize) }()
	go func() {
		defer wg.Done()
		pumpErr <- pumpToSender(tx, stdout, cfg.MaxChunk())
	}()

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-pumpErr:
	}

	// Teardown: stop the encoder so its stdout reaches EOF and the ingest pump drains,
	// then flush the sender's tail. Give the receiver a brief grace to deliver the
	// in-flight (and any recovered) tail to the sink before closing the sockets, so the
	// final access unit is not truncated.
	_ = cap.Process.Kill()
	_ = cap.Wait()
	tx.Flush()
	_ = tx.Close()
	time.Sleep(o.buffer + 300*time.Millisecond)
	_ = rx.Close()
	_ = closeSink()
	if sinkCmd != nil {
		_ = sinkCmd.Wait()
	}
	wg.Wait()

	printStats(o, tx, rx, proxy)
	return runErr
}

// runSend captures and streams to a remote receiver (the camera host).
func runSend(ctx context.Context, o *options) error {
	if o.connect == "" {
		return fmt.Errorf("-mode send requires -connect host:port")
	}
	cfg := o.meldConfig()
	tx, err := meld.NewSender(o.connect, cfg)
	if err != nil {
		return fmt.Errorf("sender: %w", err)
	}
	defer func() { _ = tx.Close() }()

	cap := captureCommand(ctx, o)
	stdout, err := cap.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cap.Start(); err != nil {
		return fmt.Errorf("start capture: %w", err)
	}
	fmt.Fprintf(os.Stderr, "meldmirror: %s -> Meld -> %s\n", sourceLabel(o), o.connect)

	done := make(chan error, 1)
	go func() { done <- pumpToSender(tx, stdout, cfg.MaxChunk()) }()

	var runErr error
	finished := false
	select {
	case <-ctx.Done():
	case runErr = <-done:
		finished = true
	}
	_ = cap.Process.Kill()
	_ = cap.Wait()
	tx.Flush()
	_ = tx.Close()
	if !finished {
		runErr = <-done
	}
	printStats(o, tx, nil, nil)
	return runErr
}

// runRecv binds a receiver and displays the recovered stream (the display host).
func runRecv(ctx context.Context, o *options) error {
	cfg := o.meldConfig()
	rx, err := meld.NewReceiver(o.listen, cfg)
	if err != nil {
		return fmt.Errorf("receiver: %w", err)
	}
	defer func() { _ = rx.Close() }()

	sink, closeSink, sinkCmd, err := openSink(ctx, o)
	if err != nil {
		return err
	}
	defer func() { _ = closeSink() }()

	fmt.Fprintf(os.Stderr, "meldmirror: listening on %s -> Meld -> %s\n", rx.LocalAddr(), sinkLabel(o))

	done := make(chan error, 1)
	go func() { done <- pumpFromReceiver(rx, sink, cfg.SymbolSize) }()

	var runErr error
	finished := false
	select {
	case <-ctx.Done():
	case runErr = <-done:
		finished = true
	}
	_ = rx.Close()
	_ = closeSink()
	if sinkCmd != nil {
		_ = sinkCmd.Wait()
	}
	if !finished {
		runErr = <-done
	}
	printStats(o, nil, rx, nil)
	return runErr
}

// openSink returns the destination for recovered chunks: ffplay's stdin (output=="play")
// or a freshly created file. It returns the writer, a close function (idempotent), the
// ffplay command if one was started, and any error.
func openSink(ctx context.Context, o *options) (io.Writer, func() error, *exec.Cmd, error) {
	if o.output == "play" {
		if _, err := exec.LookPath("ffplay"); err != nil {
			return nil, nil, nil, fmt.Errorf("ffplay not found on PATH (brew install ffmpeg): %w", err)
		}
		cmd := playCommand(ctx, o)
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return nil, nil, nil, err
		}
		if err := cmd.Start(); err != nil {
			return nil, nil, nil, fmt.Errorf("start ffplay: %w", err)
		}
		var once sync.Once
		var closeErr error
		closeFn := func() error { once.Do(func() { closeErr = stdin.Close() }); return closeErr }
		return stdin, closeFn, cmd, nil
	}

	f, err := os.Create(o.output)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create output: %w", err)
	}
	var once sync.Once
	var closeErr error
	closeFn := func() error { once.Do(func() { closeErr = f.Close() }); return closeErr }
	return f, closeFn, nil, nil
}

func sourceLabel(o *options) string {
	if o.input == "test" {
		return "synthetic " + o.size + " test pattern"
	}
	return fmt.Sprintf("camera[%s] %s@%dfps", o.device, o.size, o.fps)
}

func sinkLabel(o *options) string {
	if o.output == "play" {
		return "ffplay"
	}
	return o.output
}

// printStats reports what Meld did this run: how much the sender emitted and how much the
// receiver delivered, recovered from coding, and lost — the survivability story in numbers.
func printStats(o *options, tx *meld.Sender, rx *meld.Receiver, proxy *lossyProxy) {
	if tx != nil {
		s := tx.Stats()
		fmt.Fprintf(os.Stderr, "meldmirror: sender   source=%d repair=%d (reactive=%d, throttled=%d)\n",
			s.Source, s.Repair, s.ReactiveRepair, s.Throttled)
	}
	if proxy != nil {
		dropped, fwd := proxy.dropped(), proxy.forwarded()
		total := dropped + fwd
		var pct float64
		if total > 0 {
			pct = float64(dropped) / float64(total) * 100
		}
		fmt.Fprintf(os.Stderr, "meldmirror: relay    dropped=%d/%d (%.1f%% actual uplink loss)\n", dropped, total, pct)
	}
	if rx != nil {
		s := rx.Stats()
		fmt.Fprintf(os.Stderr, "meldmirror: receiver delivered=%d recovered=%d lost=%d wireLost=%d duplicates=%d\n",
			s.Delivered, s.Recovered, s.Lost, s.WireLost, s.Duplicates)
	}
}
