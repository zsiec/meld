package session

import (
	"testing"

	"github.com/zsiec/meld/internal/crypto"
	"github.com/zsiec/meld/internal/wire"
)

// FuzzTrialDecryptNoPanic drives the receiver-side trial-decrypt paths — tryPromote,
// authenticates, and staleStraggler — with arbitrary datagrams. These run on RAW inbound
// bytes (before the core sees them), decoding a wire symbol and AEAD-opening it, so per the
// project's no-panic rule arbitrary input must never panic. It also exercises the bound that
// a non-epoch-0 / non-opening symbol neither promotes nor spins (the methods return quickly).
func FuzzTrialDecryptNoPanic(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 8))
	f.Add(make([]byte, 1500))

	o, err := newOpenState(&SecurityConfig{Passphrase: []byte("fuzz pw"), Params: lightArgon2}, 0xABCD)
	if err != nil {
		f.Fatalf("newOpenState: %v", err)
	}
	o.established = true
	o.hsKeys = []byte("live")
	// A live opener so authenticates/staleStraggler have keys to trial against, and a pending
	// so tryPromote takes its full path.
	o.epochKeyer = epochKeyer{recvSecret: o.psk}
	o.opener, _ = crypto.NewOpener(crypto.EpochKey(o.psk, 0), 0)
	o.pending = &pendingState{keyer: epochKeyer{recvSecret: o.psk}}

	f.Fuzz(func(t *testing.T, datagram []byte) {
		if o.pending == nil { // a (vanishingly unlikely) successful promote clears it; restore
			o.pending = &pendingState{keyer: epochKeyer{recvSecret: o.psk}}
		}
		sym, err := wire.DecodeSymbol(datagram)
		if err != nil {
			return // an undecodable datagram never reaches the trial methods
		}
		_ = o.tryPromote(sym)
		_ = o.authenticates(sym)
		_ = o.staleStraggler(sym)
	})
}
