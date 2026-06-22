package session

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

// TestEncryptedDeliveryRecyclesPlaintext stresses the open-path plaintext pool (openAll decrypts
// into a pooled buffer; Read returns it to the pool after copying it out). It delivers many symbols
// through an encrypted session and verifies every full read byte-exact — interleaved with short
// reads that force a partial copy before the recycle. A use-after-recycle (a buffer reused while
// still queued, or recycled while still referenced) would corrupt a later chunk and fail the check.
func TestEncryptedDeliveryRecyclesPlaintext(t *testing.T) {
	cfg := testCfg()
	sec := &SecurityConfig{Passphrase: []byte("recycle passphrase"), EpochSize: 64, Params: lightArgon2}
	rx, err := NewReceiver("127.0.0.1:0", cfg, sec)
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}
	defer rx.Close()
	tx, err := NewSender(rx.LocalAddr(), cfg, sec)
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	defer tx.Close()

	const n = 800
	chunkLen := cfg.SymbolSize - 16
	want := make([][]byte, n)
	for i := range want {
		b := make([]byte, chunkLen)
		binary.BigEndian.PutUint32(b, uint32(i))
		for j := 4; j < chunkLen; j++ {
			b[j] = byte(i*131 + j) // a per-chunk pattern so any cross-symbol leak is visible
		}
		want[i] = b
	}
	go func() {
		for i := 0; i < n; i++ {
			tx.Write(want[i]) // read-only access (no race)
			time.Sleep(time.Millisecond)
		}
		tx.Flush()
	}()

	full := make([]byte, cfg.SymbolSize)
	short := make([]byte, 8) // forces a partial copy + recycle
	got := 0
	for got < n {
		rx.SetReadDeadline(time.Now().Add(3 * time.Second))
		buf := full
		if got%5 == 0 {
			buf = short // exercise the short-read recycle path
		}
		m, err := rx.Read(buf)
		if err != nil {
			break
		}
		id := binary.BigEndian.Uint32(buf[:4])
		if int(id) >= n {
			t.Fatalf("chunk id %d out of range — recycled buffer leaked a bad header", id)
		}
		if &buf[0] == &full[0] && !bytes.Equal(buf[:m], want[id]) {
			t.Fatalf("chunk %d corrupted — a recycled plaintext buffer leaked into delivery", id)
		}
		got++
	}
	if got != n {
		t.Fatalf("delivered %d/%d — recycle stress lost symbols", got, n)
	}
}
