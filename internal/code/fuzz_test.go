package code

import "testing"

// FuzzDecoderNoPanic drives the decoder with arbitrary systematic/repair inputs
// derived from the fuzz bytes and asserts it never panics and never reports more
// rank than its window allows. Meld's no-panic-in-library rule: arbitrary input to
// any decoder must never panic.
func FuzzDecoderNoPanic(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Add([]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1})

	f.Fuzz(func(t *testing.T, data []byte) {
		const win = 16
		const ssize = 8
		dec := NewDecoder(ssize, 0, win)
		// Walk the fuzz bytes as a tape of operations.
		for i := 0; i+2 < len(data); i += 3 {
			op := data[i]
			arg := int(data[i+1])
			key := uint16(data[i+2])
			switch op % 3 {
			case 0: // systematic
				dec.AddSystematic(uint32(arg%(win+4)), data[i:])
			case 1: // repair over a sub-window
				base := uint32(arg % win)
				n := int(data[i+2])%win + 1
				dec.AddRepair(base, n, key, data[i:])
			case 2: // repair over the full window
				dec.AddRepair(0, win, key, data[i:])
			}
			if dec.Rank() > win || dec.NumDecoded() > win {
				t.Fatalf("rank %d / decoded %d exceeds window %d", dec.Rank(), dec.NumDecoded(), win)
			}
		}
		if d := dec.Deficit(win); d < 0 || d > win {
			t.Fatalf("deficit out of range: %d", d)
		}
	})
}

// FuzzBandDecoderNoPanic drives the band-form decoder with arbitrary
// systematic/repair/skip operations (repairs bounded to the band) and asserts it
// never panics, the cursor never goes backwards, and deliveries are strictly
// increasing.
func FuzzBandDecoderNoPanic(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3})

	f.Fuzz(func(t *testing.T, data []byte) {
		const b, maxWin, ss = 8, 32, 8
		d := NewBandDecoder(ss, b, maxWin)
		prevCursor := uint32(0)
		prevDelivered := int64(-1)
		for i := 0; i+3 < len(data); i += 4 {
			switch data[i] % 4 {
			case 0:
				d.AddSystematic(d.Cursor()+uint32(data[i+1])%(maxWin+8), data[i:])
			case 1:
				d.AddRepair(d.Cursor()+uint32(data[i+1])%maxWin, int(data[i+2])%b+1, uint16(data[i+3]), data[i:])
			case 2:
				d.AddRepair(d.Cursor(), int(data[i+2])%b+1, uint16(data[i+3]), data[i:])
			case 3:
				d.Skip()
			}
			if d.Cursor() < prevCursor {
				t.Fatal("cursor went backwards")
			}
			prevCursor = d.Cursor()
			for {
				r, ok := d.Deliver()
				if !ok {
					break
				}
				if int64(r.ID) <= prevDelivered {
					t.Fatalf("delivery not increasing: %d after %d", r.ID, prevDelivered)
				}
				prevDelivered = int64(r.ID)
			}
			if d.Deficit() < 0 {
				t.Fatalf("negative deficit %d", d.Deficit())
			}
		}
	})
}
