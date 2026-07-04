# meld — build/lint/test. Guarded to work while the module has no Go packages.

GO   ?= go
PKGS := $(shell $(GO) list ./... 2>/dev/null)

.PHONY: build lint test bench check-deps check-core-imports tidy demo-mirror

build:
	@$(GO) build ./... 2>/dev/null || echo "no packages yet"

# gofmt sweeps the repo's OWN files (tracked + untracked-unignored): gitignored
# scratch areas (.claude/worktrees checkouts at older commits) are not this repo's
# lint surface and must not fail the gate.
lint:
	@files=$$(git ls-files -co --exclude-standard '*.go' 2>/dev/null); \
	if [ -n "$$files" ]; then \
		out=$$(gofmt -l $$files 2>/dev/null); if [ -n "$$out" ]; then \
		echo "gofmt needed:"; echo "$$out"; exit 1; fi; fi
	@if [ -n "$(PKGS)" ]; then $(GO) vet ./...; else echo "no packages to vet yet"; fi

# 300s: internal/flow is a battery of deterministic sim sweeps; the heavy tests run
# t.Parallel(), so wall time is well under this on a multicore box, but -race on a
# loaded single CI core needs the headroom.
test:
	@if [ -n "$(PKGS)" ]; then $(GO) test -race -count=1 -timeout 300s ./...; \
		else echo "no packages to test yet"; fi

bench:
	@if [ -n "$(PKGS)" ]; then $(GO) test -bench=. -benchmem ./...; \
		else echo "no packages to bench yet"; fi

# demo-mirror — the live latency-mirror showcase: webcam -> Meld -> screen, both ends in
# one process over loopback. Point the camera at the ffplay window for the recursive
# latency tunnel. Requires ffmpeg + ffplay on PATH (brew install ffmpeg). Pass extra flags
# via ARGS, e.g. `make demo-mirror ARGS="-loss 0.2"` or `ARGS="-input test"` (no camera).
demo-mirror:
	@$(GO) run ./cmd/meldmirror $(ARGS)

# Dependency allowlist gate: stdlib + golang.org/x/{crypto,net,sys} only. x/sys is
# admitted as a transitive dep of x/crypto (CPU-feature detection for its SIMD AEAD /
# Argon2 paths); see README, Dependencies. Checked across the platforms we ship so a
# build-tag-gated import (e.g. x/sys/cpu only on amd64) cannot slip past the host arch.
check-deps:
	@if [ -z "$(PKGS)" ]; then echo "no packages yet — skipping dep gate"; exit 0; fi; \
	bad=""; \
	for osarch in darwin/arm64 darwin/amd64 linux/amd64 linux/arm64; do \
		os=$${osarch%/*}; arch=$${osarch#*/}; \
		for imp in $$(GOOS=$$os GOARCH=$$arch $(GO) list -deps \
			-f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./... 2>/dev/null \
			| grep -v '^github.com/zsiec/meld'); do \
			case "$$imp" in \
				golang.org/x/crypto/*|golang.org/x/net/*|golang.org/x/sys/*) ;; \
				*) bad="$$bad\n  $$osarch: $$imp" ;; \
			esac; \
		done; \
	done; \
	if [ -n "$$bad" ]; then \
		printf 'check-deps: dependency outside the allowlist (stdlib + golang.org/x/{crypto,net,sys}):%b\n' "$$bad"; \
		exit 1; \
	fi; \
	echo "check-deps: ok (stdlib + golang.org/x/{crypto,net,sys})"

# internal/flow import gate: only internal/{seq,clock,rtt,wire,code,gf} + stdlib (gf is
# the GF(2^8) math beneath code). Asserts the sans-I/O core never reaches the substrate,
# crypto, the shaper, or any external module. Checked across platforms so a SIMD build
# variant cannot smuggle in an external CPU-detection dep.
check-core-imports:
	@if ! $(GO) list ./internal/flow >/dev/null 2>&1; then \
		echo "internal/flow does not exist yet — skipping"; exit 0; fi; \
	bad=""; \
	for osarch in darwin/arm64 darwin/amd64 linux/amd64 linux/arm64; do \
		os=$${osarch%/*}; arch=$${osarch#*/}; \
		for imp in $$(GOOS=$$os GOARCH=$$arch $(GO) list -deps \
			-f '{{if not .Standard}}{{.ImportPath}}{{end}}' ./internal/flow 2>/dev/null); do \
			case "$$imp" in \
				github.com/zsiec/meld/internal/seq|\
				github.com/zsiec/meld/internal/clock|\
				github.com/zsiec/meld/internal/rtt|\
				github.com/zsiec/meld/internal/wire|\
				github.com/zsiec/meld/internal/code|\
				github.com/zsiec/meld/internal/gf|\
				github.com/zsiec/meld/internal/flow) ;; \
				*) bad="$$bad\n  $$osarch: $$imp" ;; \
			esac; \
		done; \
	done; \
	if [ -n "$$bad" ]; then \
		printf 'check-core-imports: internal/flow imports outside internal/{seq,clock,rtt,wire,code,gf} + stdlib:%b\n' "$$bad"; \
		exit 1; \
	fi; \
	echo "check-core-imports: ok"

tidy:
	@$(GO) mod tidy
