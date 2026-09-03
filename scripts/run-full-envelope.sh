#!/usr/bin/env bash

set -euo pipefail

repo_dir=$(cd "$(dirname "$0")/.." && pwd)
run_stamp=$(date -u +%Y%m%dT%H%M%SZ)
out_dir=${OUT_DIR:-"$repo_dir/scratchpad/full-envelope-$run_stamp"}
shard_count=${SHARDS:-64}
job_count=${JOBS:-8}
clips="internal/shape/testdata/bbb_bframes.h264,internal/shape/testdata/bbb.h265,internal/shape/testdata/bbb.obu"

if [[ $shard_count -lt 1 || $job_count -lt 1 ]]; then
	echo "SHARDS and JOBS must both be positive" >&2
	exit 2
fi

mkdir -p "$out_dir/bin" "$out_dir/shards" "$out_dir/logs" "$out_dir/provenance"
bench_bin="$out_dir/bin/glassbench"

if [[ ! -x $bench_bin ]]; then
	(cd "$repo_dir" && go build -o "$bench_bin" ./cmd/glassbench)
fi
(cd "$repo_dir" && git status --short >"$out_dir/provenance/git-status.txt")
(cd "$repo_dir" && git diff --binary >"$out_dir/provenance/worktree.patch")
(cd "$repo_dir" && git rev-parse HEAD >"$out_dir/provenance/git-revision.txt")
shasum -a 256 "$bench_bin" >"$out_dir/provenance/binaries.sha256"

cat >"$out_dir/provenance/run.txt" <<EOF
suite=full-envelope
clips=$clips
shards=$shard_count
jobs=$job_count
repetitions=3
source_mbps=8
shared_wire_mbps=10.8
meld_max_mbps=10.8
buffer_floor_ms=0
deadline_arbiter=true
EOF

source_ids=(bbb-bframes-h264 bbb-h265 bbb-obu)

shard_complete() {
	local shard_dir=$1
	local source_id
	for source_id in "${source_ids[@]}"; do
		if [[ ! -f "$shard_dir/$source_id/COMPLETE.json" ]]; then
			return 1
		fi
		if ! jq -e 'all(.[]; .Failed == 0 and .Seeds == 3)' "$shard_dir/$source_id/frontier_rows.json" >/dev/null; then
			return 1
		fi
	done
	return 0
}

run_shard() {
	local shard=$1
	local tag
	local shard_dir
	local log_path
	printf -v tag '%03d' "$shard"
	shard_dir="$out_dir/shards/shard-$tag"
	log_path="$out_dir/logs/shard-$tag.log"
	if shard_complete "$shard_dir"; then
		echo "shard $tag already complete"
		return 0
	fi
	echo "shard $tag starting"
	(cd "$repo_dir" && "$bench_bin" \
		-publishsuite full-envelope \
		-publishclips "$clips" \
		-reportdir "$shard_dir" \
		-frontiershards "$shard_count" \
		-frontiershard "$shard" \
		-reps 3 \
		-buf 0 \
		-mbps 8 \
		-maxmbps 10.8 \
		-wirembps 10.8 \
		-deadlinearbiter) >"$log_path" 2>&1
	if ! shard_complete "$shard_dir"; then
		echo "shard $tag exited without all three completion markers" >&2
		return 1
	fi
	echo "shard $tag complete"
}

flush_batch() {
	local index
	local failed=0
	for index in "${!batch_pids[@]}"; do
		if ! wait "${batch_pids[$index]}"; then
			echo "shard ${batch_shards[$index]} failed; inspect its log" >&2
			failed=1
		fi
	done
	batch_pids=()
	batch_shards=()
	if [[ $failed -ne 0 ]]; then
		return 1
	fi
}

batch_pids=()
batch_shards=()
had_failure=0
for ((shard = 0; shard < shard_count; shard++)); do
	run_shard "$shard" &
	batch_pids+=("$!")
	batch_shards+=("$shard")
	if [[ ${#batch_pids[@]} -ge $job_count ]]; then
		if ! flush_batch; then
			had_failure=1
		fi
	fi
done
if [[ ${#batch_pids[@]} -gt 0 ]]; then
	if ! flush_batch; then
		had_failure=1
	fi
fi
if [[ $had_failure -ne 0 ]]; then
	echo "one or more shards failed; rerun with the same OUT_DIR to resume" >&2
	exit 1
fi

(cd "$repo_dir" && "$bench_bin" \
	-publishsuite full-envelope \
	-publishclips "$clips" \
	-mergefrontier "$out_dir/shards" \
	-reportdir "$out_dir/merged" \
	-frontiershards "$shard_count" \
	-reps 3 \
	-buf 0 \
	-mbps 8 \
	-maxmbps 10.8 \
	-wirembps 10.8 \
	-deadlinearbiter)

echo "complete audited envelope: $out_dir/merged/README.md"
