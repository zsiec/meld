# Encryption — a modern, survivable crypto layer

Meld is a clean-sheet protocol, so it gets to skip the crypto debt SRT and RIST
carry. This note pins the v1 design and records why each choice beats the baseline.
It is a decision record (like [`docs/substrate.md`](substrate.md)), not the byte-pinned
wire spec — the handshake message bytes get fixed in [`docs/wireformat.md`](wireformat.md)
when built, behind the version nibble, the way N0 pinned the symbol format.

---

## 0. The decision, in one block

```
Handshake (host layer):
  Argon2id(passphrase) ──► PSK ──► PQ-converted Noise PSK pattern
  forward secrecy + post-quantum by CHAINING X25519 then ML-KEM-768
  through successive HKDF/MixKey steps; both ephemerals + the ML-KEM
  ciphertext bound into the transcript hash; mac1/mac2 anti-amplification.
Record layer (encrypt-then-code, per source symbol):
  ChaCha20-Poly1305; deterministic 96-bit nonce = epoch‖src_index (never
  on the wire); refuse-to-reuse ⇒ rekey (bump epoch) before src_index
  wraps 2³²; epoch bound into the AAD as symbol-identity binding (not key commitment — §5.4).
Key schedule:  HKDF-SHA256 throughout.
Primitives:    crypto/{mlkem,hkdf,ecdh,pbkdf2} (stdlib) + x/crypto
               {argon2,chacha20poly1305}.  Zero new dependencies.
```

The one deferred fork is the **front** of the handshake: v1 stretches the passphrase
with Argon2id into a PSK; **CPace** (a balanced PAKE) is the planned v2 hardening for the
human-passphrase threat model (§8).

---

## 1. Why — the baseline this beats

Both SRT and RIST bolt confidentiality onto a static pre-shared passphrase, and it
shows:

- **SRT:** PBKDF2-HMAC-**SHA1** (2048 iterations) → AES Key Wrap (RFC 3394) of a random
  SEK → **AES-CTR with no authentication** for years (a bit-flip is undetectable;
  AES-GCM only arrived in v1.5.4). Static PSK ⇒ **no forward secrecy**: leak the
  passphrase and every past stream is readable. No post-quantum story.
- **RIST:** PBKDF2-HMAC-SHA256 (1024 iterations) → **AES-CTR** (also unauthenticated) in
  the simple/main PSK path; or **EAP-SRP** (a 1998-era PAKE) deriving a session key; or a
  bolt-on **DTLS 1.2** tunnel (forward secrecy, but X.509/PKI, ~37 B/packet, DTLS-1.2-not-1.3,
  first-datagram-binding DoS).

Three gaps recur in every PSK path: **no forward secrecy, no post-quantum, a weak
password KDF.** Meld closes all three while staying pure-Go and dependency-light.

---

## 2. Threat model & deployment

- **Topology:** point-to-point contribution — one sender ↔ one receiver, provisioned
  endpoints. (Group/one-to-many keying and untrusted-relay pollution resistance are out
  of scope for v1; §8.)
- **Identity bootstrap:** a shared **passphrase**, which may be **human-entropy** — so
  offline-dictionary resistance is a design concern, not an afterthought.
- **Protected:** media payload confidentiality + integrity, end to end, with forward
  secrecy and post-quantum (harvest-now-decrypt-later) resistance.
- **Visible (accepted, same as SRT/RIST):** the cleartext coding header — flow, epoch,
  window, coefficients, priority, deadline, frame boundaries — so the core and any relay
  can route/recode without keys. An eavesdropper sees packet sizes, frame structure, and
  priority tiers (traffic analysis), not content. The header is **authenticated** (AAD,
  §5.4); it is not encrypted.
- **Out of scope v1:** anonymity/deniability; in-network (per-hop) pollution detection;
  metadata confidentiality.

---

## 3. Where it sits — the core stays crypto-blind

The import-gate rule is non-negotiable: `internal/flow` imports only
`seq/clock/rtt/wire/code/gf` + stdlib. **Crypto never enters the core.** It treats
payloads as opaque bytes already, so:

- **A new `internal/crypto` package** holds the pure, deterministic primitives and the
  Noise state transitions as pure functions — sans-I/O, no clock, no socket, no goroutine
  (the discipline ristgo's crypto package already proves works). The **host**
  (`internal/session`) owns the keys, sequences the handshake, and does the I/O, exactly
  as it owns `clock.RealClock` and the timer wheel.
- **The handshake rides the existing 2-message exchange.** The cross-host clock-offset
  handshake (N4) is the natural carrier for the wire-format epoch and an anti-amplification
  cookie. The crypto handshake subsumes it: it establishes keys *and* carries the clock
  offset and the cookie, as new version-nibbled message types alongside the clock
  probe/echo.
- **The record layer wraps the Write/Read boundary.** `Sender.Write` AEAD-seals the media
  chunk *before* it becomes a source symbol; the receiver AEAD-opens *after* the core
  delivers. The core's only change is to **report each delivered symbol's `src_index`**
  (which it already knows — it is the delivery cursor) so the host can derive the
  deterministic nonce; that reports an id, it does not make the core key-aware. The import
  gate is untouched.

---

## 4. The handshake — Argon2id-PSK → hybrid-PQ Noise

### 4.1 Password stretch — Argon2id (RFC 9106)

`PSK = Argon2id(passphrase, salt, params)` via `golang.org/x/crypto/argon2`, replacing
SRT's PBKDF2-SHA1. Argon2id is **memory-hard**, so each offline guess costs real memory ×
time, not a cheap hash. Recommended starting params (RFC 9106 §4, the "second recommended"
profile): **m = 64 MiB, t = 3, p = 4**, tunable up — contribution endpoints are provisioned,
so a stronger profile (up to the 2 GiB option) is affordable.

Crucially, the PSK is a **long-term** derivation (WireGuard's static-key analogue):
computed **once per peer at startup and cached**, salted by a deployment-configured value
(a fixed salt prevents cross-deployment rainbow tables; per-session freshness comes from
the ephemerals, not the salt). So Argon2id's cost is paid once, not per handshake or per
packet — which is also what makes mac1 a cheap DoS gate (§4.4).

### 4.2 The pattern — a PSK Noise handshake, no static identity keys

With identity bootstrapped from a passphrase (no separate per-host static keypairs in v1),
the fit is an **`NNpsk0`-style Noise pattern** (The Noise Protocol Framework, rev. 34):
two ephemerals + the PSK, where the PSK supplies mutual authentication and the ephemeral
exchange supplies **forward secrecy**. No X.509, no PKI, no certificates. The exact token
ordering is fixed at implementation against the Noise spec, with test vectors, and pinned
in `wireformat.md` (as N0 did for the symbol header).

### 4.3 Post-quantum — chain X25519 then ML-KEM-768 (don't hand-roll X-Wing)

PQNoise (Angel–Dowling–Hülsing–Schwabe–Weber, CCS 2022; eprint 2022/539) proves that the
**generic DH→KEM substitution preserves confidentiality, authenticity, and forward secrecy**
in the fACCE model — and its reference implementation is in Go. The KEM shared secret
enters **exactly where the DH output did**: the sender encapsulates under the peer's public
KEM key, sends the ciphertext, and **both parties hash the ciphertext into the transcript
hash `h` (MixHash) and fold the shared secret into the chaining key `ck` via HKDF (MixKey)**
(PQNoise's `ekem`/`skem` tokens are the analogues of Noise's `ee`/`es`/`se`/`ss`).

For the **hybrid**, Meld does **not** package an X-Wing KEM. X-Wing
(`draft-connolly-cfrg-xwing-kem`, still an *individual* I-D) is the standardized
X25519+ML-KEM-768 combiner, but it deliberately **omits the ML-KEM ciphertext from the
combiner** — a performance trick that "relies crucially on the Fujisaki–Okamoto transform
inside ML-KEM-768" and "is not known to be safe in the general case." Hand-rolling that
omission is a subtle correctness trap, and the production Go impl (Cloudflare CIRCL) is
outside the allowlist anyway.

Instead, **chain both secrets through successive `MixKey`/HKDF steps** — `MixKey(ss_X25519)`
then `MixKey(ss_MLKEM)` into the same `ck`, with both ephemeral public keys and the ML-KEM
ciphertext mixed into `h`. HKDF-chaining *is* a sound hybrid combiner (an attacker must
break **both** layers), it is precisely what PQ-WireGuard (`KDF1(C, shk)` per secret;
Hülsing et al., IEEE S&P 2021) and Rosenpass (inject the PQ secret as the WireGuard PSK via
HKDF — "cryptographically no less secure than WireGuard alone") do, and it sidesteps the
X-Wing ciphertext-omission pitfall entirely.

**Binding hazards to respect** (PQ-WireGuard 2021 + the binding-secure redesign, Hashimoto–
Katsumata–Niot–Wiggers, eprint 2025/1758, S&P 2026; KEM-binding issues per Cremers et al.,
CCS 2024): a KEM is "semantically but not syntactically the same as DH," and naive porting
caused ad-hoc bugs. So **bind the full transcript** (both ephemeral public keys + the ML-KEM
ciphertext into `h`) and, to foreclose unknown-keyshare/downgrade, **bind the session to the
negotiated parameters** — the PQ-WireGuard move of folding `H(static_i ⊕ static_r)` into the
PSK is the template once static keys land (§8); in v1, the negotiated suite + flow id are
mixed into `ck`.

### 4.4 Anti-amplification — mac1 (PSK-gated) + mac2 (cookie)

ML-KEM-768 makes the first message large (encapsulation key ≈ 1184 B; ciphertext ≈ 1088 B),
which is itself anti-amplification (the response is no larger than the request, so the
amplification factor is ≈ 1). On top of that, WireGuard's two-MAC scheme (Donenfeld, NDSS
2017):

- **mac1** = a keyed MAC over the handshake message under a key derived from the cached PSK.
  Because the PSK is already in memory (§4.1), this is a **cheap per-handshake check that
  gates the expensive ML-KEM decapsulation on proof of passphrase knowledge** — a flood of
  forged first messages is dropped before any asymmetric crypto runs.
- **mac2** = a cookie for **return-routability** under load (implemented; off until the
  responder's per-window handshake-attempt count crosses `SecurityConfig.CookieThreshold`):
  the responder replies with a cheap keyed-hash cookie of the source address (encrypted
  under the PSK in a `0x8` message), the initiator echoes it as mac2 in a retried message 1,
  and only a matching cookie passes. This gates work on a real (non-spoofed) source address
  — the WireGuard analogue of QUIC's Retry, lighter for a sans-I/O host than QUIC tokens.
  The cookie secret rotates (~1 s) so an observed cookie cannot be replayed indefinitely.

### 4.5 Output — the key schedule

The handshake's transcript-bound `ck` is split (HKDF-Expand, `crypto/hkdf`, RFC 5869) into
**directional traffic secrets** (sender→receiver and receiver→sender, never shared) and the
**epoch-0 key**. Subsequent epoch keys ratchet forward from the traffic secret (§6), so
compromise of one epoch key does not yield earlier ones.

---

## 5. The record layer — encrypt-then-code

### 5.1 Placement — encrypt the source symbol, code the ciphertext

This is the one place Meld diverges from SRT/RIST, because it is *coded*. Encryption is
applied to each **source symbol** *before* coding:

1. `c_i = AEAD-Seal(K_epoch, nonce_i, media_chunk_i, aad_i)` → ciphertext ‖ 16-byte tag.
2. The coder treats the whole AEAD output `(ciphertext ‖ tag)` as the opaque, fixed-size
   symbol payload. Systematic symbols carry `c_i` verbatim; **repair symbols are GF(2⁸)
   linear combinations of the `c_i`** ([`docs/coding.md`](coding.md)).
3. **A recoding relay recombines `c_i` with no key** — RLNC's superpower survives, because
   the linear algebra is over ciphertext bytes (the recoding-relay mode).
4. The receiver solves the linear system → recovers each `(ciphertext ‖ tag)` → `AEAD-Open`
   verifies and decrypts.

Cost: **+16 bytes per source symbol** (the tag is carried as coded payload bytes and
reconstructed by the linear solve like any other payload byte). The coding symbol size is
`media_chunk + 16`.

### 5.2 AEAD — ChaCha20-Poly1305 (RFC 8439)

`golang.org/x/crypto/chacha20poly1305`. **Constant-time in pure Go without AES hardware** —
the right fit for a deterministic, portable, sans-I/O core, and the WebRTC/MoQ-era default.
**AES-256-GCM** (`crypto/cipher`) is the alternative on links with guaranteed AES-NI
(server-to-server contribution). **AEGIS** (`draft-irtf-cfrg-aegis-aead`) is faster but
AES-based (needs hardware for speed *and* constant-time) with no allowlist Go impl —
**skip for v1, track.**

### 5.3 Nonce — deterministic, never on the wire

`nonce_i = epoch ‖ src_index` (96-bit; `epoch` is the wire field, `src_index` the source
id). The receiver derives it from the delivered id + the current epoch, so **no nonce bytes
travel** — only the +16 tag. **XChaCha20's 192-bit nonce buys nothing here:** its purpose
is collision-safety for *random* nonces, and deterministic counters cannot collide
(`draft-irtf-cfrg-xchacha`).

The discipline is RIST's `ErrIVExhausted`, generalized: **refuse to reuse `(key, nonce)`**
— bump the epoch (rekey) before `src_index` wraps 2³² within an epoch. ChaCha20-Poly1305's
confidentiality/integrity limits (`draft-irtf-cfrg-aead-limits`) sit far above a 2³²
per-epoch budget, so the bound is comfortable.

### 5.4 AAD + epoch binding

The cleartext source-symbol metadata (`flow`, `epoch`, `src_index`, `priority`, `deadline`,
frame descriptor) is the **AAD** — authenticated, not encrypted, so it cannot be tampered
without detection and a relay/the core can still read it.

ChaCha20-Poly1305 and AES-GCM are **not key-committing**, which opens partitioning-oracle
attacks in *multi-key* settings where an attacker controls or guesses the keys (Albertini et
al., USENIX Security 2022; Len–Grubbs–Ristenpart, USENIX Security 2021). Meld does not expose
that surface: every key derives from the PSK + the hybrid handshake (never attacker-chosen),
and the receiver derives exactly the one expected key per delivered `src_index`. The
re-handshake path does trial-decrypt a symbol under the receiver's own *pending* and *live*
keys, but those are still its own handshake-derived keys, so there is no partitioning oracle.

Binding `flow`/`epoch`/`src_index` into the AAD is **symbol-identity binding, not key
commitment**: it makes a tampered or misrouted cleartext identity fail authentication. What
keeps an epoch's ciphertext from ever verifying under a different epoch is that each epoch
uses a **distinct ratcheted key**, not the AAD — even with a key-update overlap where two
epochs are briefly live, their keys differ. If a committing guarantee were ever required (it
is not, given the above), the mechanism would be a committing AEAD or an explicit
key-commitment tag; the AAD epoch field does not provide one.

### 5.5 Integrity through the code

Because the AEAD is computed *before* coding, integrity is **end-to-end via the per-source
tags**: a forged repair symbol (or a polluted coefficient) corrupts the linear solve, so the
recovered `(ciphertext ‖ tag)` fails its Poly1305 tag and is rejected. The consequence to
state plainly: the **repair symbols' coding headers are cleartext and not individually
authenticated** — they are protected end-to-end, not per-hop. That is the exact price of
recode-without-keys, and it is acceptable for point-to-point. **In-network (per-hop)
pollution detection via homomorphic MACs is a documented future item** (§8), not v1.

---

## 6. Key lifecycle — the epoch *is* the key update

The wire `epoch` field (`wire.Symbol.Epoch` / `wire.Feedback.Epoch`, reserved since N0,
documented as "bumps on flow reset / key update") becomes the **key epoch**. Each epoch's
traffic key is `HKDF-Expand(traffic_secret, "meld v1 epoch" ‖ epoch ‖ direction)`. Rekey
(bump epoch) triggers on any of: `src_index` approaching 2³², a time interval, or an explicit
request. Ratcheting the traffic secret forward on each epoch (and deleting the prior secret)
gives **intra-session forward secrecy** — a key compromise does not unwind earlier epochs.
16-bit epoch × 2³² symbols/epoch is far more headroom than any flow needs.

---

## 7. The Go primitive map (all allowlist-clean)

Everything is stdlib (Go 1.24+) or `golang.org/x/crypto` — **no new dependency, no cgo**.
Tellingly, `crypto/tls` already ships `X25519MLKEM768` by default (Go 1.24), proving the
pairing is production-grade in Go; Meld uses `crypto/mlkem` directly under its own sans-I/O
UDP handshake rather than `crypto/tls`.

| Need | Primitive | Source | Spec |
|---|---|---|---|
| ML-KEM-768 | `crypto/mlkem` (`GenerateKey768`, `EncapsulationKey768`, `Encapsulate`/`Decapsulate`) | stdlib 1.24 | FIPS 203 |
| HKDF | `crypto/hkdf` (Extract/Expand) | stdlib 1.24 | RFC 5869 |
| X25519 | `crypto/ecdh` | stdlib | RFC 7748 |
| Password KDF | `argon2.IDKey` | x/crypto | RFC 9106 |
| AEAD | `chacha20poly1305` / `crypto/cipher` (AES-GCM) | x/crypto / stdlib | RFC 8439 |
| (legacy KDF, if needed) | `crypto/pbkdf2` | stdlib 1.24 | RFC 8018 |

There is **no stdlib Noise and no stdlib CPace** — the Noise/PQNoise handshake is
hand-rolled (the WireGuard precedent; srtgo/ristgo hand-roll their handshakes too). Sizes:
ML-KEM-768 encapsulation key 1184 B, ciphertext 1088 B, shared secret 32 B; X25519 32 B.

---

## 8. Built, and open forks

**Built end-to-end** (`-race` green; tests in `internal/{crypto,session}` and `e2e_test.go`):
the Argon2id-PSK → hybrid-PQ-Noise handshake and the ChaCha20-Poly1305 encrypt-then-code
record layer, on **single-path, sliding-profile, and multipath** flows (the handshake rides
path 0); **epoch rotation** (the key ratchets every `EpochSize` symbols, epoch derived from
the source id — `TestEncryptedEpochRotation`); and the **mac2 cookie** under load
(`TestEncryptedCookieUnderLoad`, `TestCookieRoundTrip`). The PSK is stretched once and
cached, so the handshake never re-runs Argon2id. The wire format is pinned in
[`docs/wireformat.md`](wireformat.md).

Still open:

- **v1 Argon2id-PSK vs v2 CPace (decided: v1 now, CPace later).** For human-entropy
  passphrases the principled answer is a **balanced PAKE** — CFRG selected **CPace**
  (`draft-irtf-cfrg-cpace`) as the balanced PAKE and **OPAQUE** (RFC 9807) as the augmented
  one; OPAQUE's per-client registration record does not fit a symmetric same-passphrase
  link, so CPace is the correct shape. A PAKE caps an attacker to **one online guess per
  active interaction**, closing the offline-dictionary gap that Argon2id only makes
  *expensive*, not impossible. v1 ships Argon2id-PSK because: the only allowlist-adjacent Go
  CPace (`filippo.io/cpace`) is experimental, pre-v1, and built on an **old** draft, not the
  current one; CPace needs a prime-order group (ristretto255) not in the allowlist; and
  **PQ-augmenting a PAKE is not a well-trodden, proven construction** (PQNoise covers
  PSK-Noise, not PAKE+KEM). When a vetted PQ-PAKE composition and a pure-Go ristretto255
  exist, CPace replaces the Argon2id-PSK front with no change to the record layer or the PQ
  story. (Either way, **online guessing still needs rate-limiting/lockout** — a PAKE bounds
  offline, not online, attempts.)
- **In-network pollution detection** (homomorphic MACs for untrusted recoding relays) —
  future; v1 detects pollution end-to-end via source tags only.
- **Static per-host identity keys** — v1 is passphrase-only (auth = PSK). Adding static
  keypairs (an `IK`-style pattern) strengthens identity and enables the
  `H(static_i ⊕ static_r)` unknown-keyshare binding; reserved behind the version nibble.
- **Group / one-to-many keying** — out of scope (point-to-point contribution only).
- **AEGIS** — track; revisit if a constant-time pure-Go impl lands in the allowlist.

---

## 9. References (cite the spec/behavior in code, never a library path — the ristgo rule)

- **Handshake / PQ:** The Noise Protocol Framework (rev. 34); **PQNoise** (Angel, Dowling,
  Hülsing, Schwabe, Weber, CCS 2022 — eprint 2022/539); **PQ-WireGuard** (Hülsing, Ning,
  Schwabe, Weber, Zaverucha, IEEE S&P 2021 — eprint 2020/379) and its binding-secure redesign
  (Hashimoto, Katsumata, Niot, Wiggers, eprint 2025/1758, S&P 2026); **Rosenpass**
  (PQ key-exchange, additive WireGuard PSK); **X-Wing** (`draft-connolly-cfrg-xwing-kem`);
  KEM-binding (Cremers et al., CCS 2024); **WireGuard** mac1/mac2 cookie (Donenfeld, NDSS 2017).
- **PAKE:** CFRG PAKE selection (2020); **CPace** (`draft-irtf-cfrg-cpace`, balanced);
  **OPAQUE** (RFC 9807, augmented).
- **Primitives:** **FIPS 203** (ML-KEM); **RFC 9106** (Argon2); **RFC 5869** (HKDF);
  **RFC 7748** (X25519); **RFC 8439** (ChaCha20-Poly1305); `draft-irtf-cfrg-xchacha`;
  `draft-irtf-cfrg-aead-limits`; `draft-irtf-cfrg-aegis-aead`.
- **Committing AEAD:** Albertini, Duong, Gueron, Kölbl, Luykx, Schmieg (USENIX Security 2022);
  partitioning oracles (Len, Grubbs, Ristenpart, USENIX Security 2021).
- **Baselines (behavior):** SRT (PBKDF2-SHA1 → AES Key Wrap RFC 3394 → AES-CTR/-GCM);
  RIST/VSF TR-06 (PSK AES-CTR; EAP-SRP RFC 5054; DTLS 1.2) via `srtgo`/`ristgo`.
