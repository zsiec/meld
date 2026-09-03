# Encryption

Meld can authenticate and encrypt a point-to-point flow when both endpoints provide the
same passphrase and security parameters. The session host owns cryptography; the coding
core continues to process opaque source bytes.

The byte-level messages are defined in [wireformat.md](wireformat.md).

## Construction

```text
passphrase + salt --Argon2id--> PSK

two-message authenticated key exchange:
    ephemeral X25519
  + ephemeral ML-KEM-768
  + PSK
  + transcript-bound HKDF-SHA256

source record:
  ChaCha20-Poly1305(plaintext, nonce(epoch, source ID), AAD(flow, epoch, source ID))
  then systematic coding / repair
```

Default Argon2id parameters are 64 MiB, three iterations, and four lanes. The result is
cached for the session rather than recomputed per handshake message. Deployments should
use distinct salts and sufficiently strong passphrases; Argon2id raises the cost of an
offline guess but does not turn a weak passphrase into a high-entropy key.

The handshake follows an NNpsk0-shaped transcript. The initiator and responder contribute
fresh X25519 keys; ML-KEM-768 supplies the post-quantum component. Both shared secrets are
mixed into the chaining key with successive HKDF steps. Public keys, the KEM ciphertext,
the flow-specific prologue, and the sender's epoch size are included in the transcript.
A PSK-derived `mac1` rejects unauthenticated requests before expensive responder work.

Under configured load, the receiver may require a short-lived `mac2` cookie bound to the
peer address before processing a retry. `CookieThreshold` controls that gate; its default
effectively disables cookies for normal point-to-point operation.

## Encrypt then code

Each application chunk is sealed before it enters the erasure coder. The 16-byte
Poly1305 tag is therefore part of the coded source symbol and can be reconstructed by a
repair equation. A relay or coding node can operate on ciphertext without holding the
traffic key. The receiver decodes first and authenticates each recovered source symbol
before delivering plaintext.

`SymbolSize` remains the maximum coded-source size, so an encrypted application chunk is
limited to `SymbolSize - 16` bytes. Oversized writes return `ErrChunkTooLarge`.

The nonce is deterministic and is not transmitted. Its significant components are the
key epoch and source ID. A sealer requires strictly increasing source IDs and refuses
reuse. The host derives a new directional key at `EpochSize` source-symbol intervals and
ratchets the traffic secret forward, discarding access to earlier epoch keys.

The authenticated data contains only values reconstructable after coding: flow ID, key
epoch, and source ID. Priority, deadline, and frame metadata are not in the record AAD,
because a recovered source symbol has no copy of its original wire header. They remain
protocol inputs rather than cryptographically protected media metadata.

## Control authentication

Encrypted sessions also authenticate feedback and clock messages. Each direction has a
separate HMAC-SHA256 key. A control datagram carries a monotonic sequence number and a
truncated tag; the receiver applies a 64-entry sliding replay window. Control messages
are suppressed until the handshake establishes these keys.

Handshake messages and cookie replies retain their own framing and authentication rules.

## Re-handshake behavior

The receiver does not replace a live session merely because it receives a new handshake
request. It keeps the candidate session pending until a systematic symbol authenticates
under the candidate keys, then promotes it and rebuilds the receive core. A bounded guard
window rejects old-session datagrams still in flight. This permits sender restart without
letting a replayed first handshake message displace working keys.

## Configuration

Public `meld.Config` exposes:

- `Passphrase` and `Salt`;
- `Argon2Time`, `Argon2MemoryKiB`, and `Argon2Threads`;
- `EpochSize`;
- `CookieThreshold`.

An empty passphrase selects cleartext operation. Both endpoints must agree on the
passphrase, salt, and effective Argon2id parameters. The sender carries its epoch size in
the authenticated handshake, so the receiver adopts the sender's value.

## Security scope and limitations

- The design is for provisioned point-to-point links. It does not provide group keying,
  anonymity, or metadata confidentiality.
- Authentication is passphrase-based; there are no separate long-term host identities or
  certificates.
- A human-strength passphrase remains subject to offline guessing. The protocol does not
  implement a PAKE or online lockout policy.
- Coding headers are visible. Flow identity is record-authenticated, but priority,
  deadlines, frame descriptors, and repair headers are not individually authenticated.
- Corrupted repair can poison a decode, but the recovered source then fails its AEAD tag
  and is not delivered. There is no per-hop homomorphic pollution check.
- Traffic analysis can reveal packet sizes, timing, coding windows, and priority classes.

## Primitive references

- Argon2id: RFC 9106
- HKDF-SHA256: RFC 5869
- X25519: RFC 7748
- ML-KEM-768: FIPS 203
- ChaCha20-Poly1305: RFC 8439
- Noise protocol framework, revision 34
- PQNoise hybrid-handshake analysis, CCS 2022
- Sliding anti-replay window: RFC 6479
