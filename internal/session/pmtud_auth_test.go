package session

import (
	"testing"

	"github.com/zsiec/meld/internal/crypto"
	"github.com/zsiec/meld/internal/wire"
)

func TestMTUProbeBodySizeAccountsForControlOverhead(t *testing.T) {
	const candidate = 1200
	if got := mtuProbeBodySize(candidate, controlState{}); got != candidate {
		t.Fatalf("cleartext probe body size = %d, want %d", got, candidate)
	}

	ctl := controlState{active: true}
	ctl.setKeys([]byte("send control key"), []byte("recv control key"))
	body := wire.EncodeMTUProbe(nil, 1, mtuProbeBodySize(candidate, ctl))
	sealed, ok := ctl.seal(body)
	if !ok {
		t.Fatal("active keyed control plane refused to seal probe")
	}
	if len(sealed) != candidate {
		t.Fatalf("sealed probe len = %d, want candidate %d", len(sealed), candidate)
	}
	if len(body)+crypto.ControlOverhead != len(sealed) {
		t.Fatalf("body/control overhead mismatch: body=%d sealed=%d overhead=%d",
			len(body), len(sealed), crypto.ControlOverhead)
	}
}
