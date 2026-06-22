package flow

// ECN is the two-bit Explicit Congestion Notification codepoint of a received datagram
// (RFC 3168 §5) — the network's congestion signal, read by the host from the IP header and
// handed to Receiver.FeedSymbolECN. Meld marks its datagrams ECT(1), the L4S identifier
// (RFC 9331), so an L4S-aware AQM can set CE (Congestion Experienced) BEFORE a standing
// queue forms; the receiver echoes the CE-marked fraction in feedback and the congestion
// controller reduces the rate proportionally (RFC 9330, DCTCP-style). A mark is NEVER a
// reason to add repair (RFC 9265): it slows the sender, the FEC sizer stays loss-driven.
type ECN uint8

// ECN codepoints — the low two bits of the IP differentiated-services field (RFC 3168 §5).
const (
	NotECT ECN = 0b00 // not an ECN-capable transport
	ECT1   ECN = 0b01 // ECN-capable, the L4S identifier (what Meld sends)
	ECT0   ECN = 0b10 // ECN-capable, classic ECN
	CE     ECN = 0b11 // congestion experienced (set by an AQM under load)
)
