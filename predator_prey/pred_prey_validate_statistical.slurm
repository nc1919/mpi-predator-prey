#!/bin/bash
#SBATCH --job-name=predprey_stats_validate
#SBATCH --partition=standard
#SBATCH --qos=standard
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1
#SBATCH --time=00:30:00
#SBATCH --output=%x-%j.out
#SBATCH --error=%x-%j.err

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="${PREDPREY_SOURCE_DIR:-$SCRIPT_DIR}"
RUN_BASE="${PREDPREY_RUN_BASE:-$SCRIPT_DIR/.runs}"
RUN_DIR="${RUN_BASE%/}/predprey_stats_validate_${SLURM_JOB_ID}"
BUILD_DIR="$RUN_DIR/build"
RESULTS_DIR="$RUN_DIR/results"

RUNTIME_SOURCE="${PREDPREY_RUNTIME_SOURCE:-$SOURCE_DIR/framework/actor_model_fast_validate.c}"
MODEL_SOURCE="${PREDPREY_MODEL_SOURCE:-$SOURCE_DIR/model/predator_prey_fast_validate.c}"
TARGET_NAME="${PREDPREY_TARGET_NAME:-predprey_actor_validate}"

COMPILER_MODULE="${PREDPREY_COMPILER_MODULE:-PrgEnv-gnu}"
MPI_MODULE="${PREDPREY_MPI_MODULE:-cray-mpich}"
CPU_MODULE="${PREDPREY_CPU_MODULE:-craype-x86-rome}"
CC_BIN="${PREDPREY_CC:-cc}"

ALLOC_TASKS_PER_NODE_RAW="${SLURM_TASKS_PER_NODE:-8}"
ALLOC_TASKS_PER_NODE_RAW="${ALLOC_TASKS_PER_NODE_RAW%%(*}"
ALLOC_TASKS_PER_NODE_RAW="${ALLOC_TASKS_PER_NODE_RAW%%,*}"
TASKS_PER_NODE="${PREDPREY_TASKS_PER_NODE:-$ALLOC_TASKS_PER_NODE_RAW}"

SEED_LIST="${PREDPREY_SEED_LIST:-123456 123457 123458}"
RANK_LIST="${PREDPREY_RANK_LIST:-1 2 4}"
GRID_SIZE="${PREDPREY_GRID_SIZE:-10}"
REGION_SIDE="${PREDPREY_REGION_SIDE:-5}"
MAX_PER_CELL="${PREDPREY_MAX_PER_CELL:-100}"
INITIAL_PREY="${PREDPREY_INITIAL_PREY:-120}"
INITIAL_PREDATORS="${PREDPREY_INITIAL_PREDATORS:-30}"
STEPS_PER_DAY="${PREDPREY_STEPS_PER_DAY:-10}"
DAYS="${PREDPREY_DAYS:-5}"
VALIDATE_STRIDE_DAYS="${PREDPREY_VALIDATE_STRIDE_DAYS:-1}"
EXTRA_CFLAGS="${PREDPREY_EXTRA_CFLAGS:-}"

TARGET="$BUILD_DIR/$TARGET_NAME"
CONFIG_TXT="$RESULTS_DIR/validation_config.txt"
RUN_SUMMARY="$RESULTS_DIR/run_summary.csv"
TRAJECTORY_RAW="$RESULTS_DIR/trajectory_raw.csv"
TRAJECTORY_SUMMARY="$RESULTS_DIR/trajectory_summary.csv"
RANK_SUMMARY="$RESULTS_DIR/rank_summary.csv"

mkdir -p "$BUILD_DIR" "$RESULTS_DIR"

module -q restore >/dev/null 2>&1 || true
if [ -n "$COMPILER_MODULE" ]; then
  module -q load "$COMPILER_MODULE"
fi
if [ -n "$MPI_MODULE" ]; then
  module -q load "$MPI_MODULE"
fi
if [ -n "$CPU_MODULE" ]; then
  module -q load "$CPU_MODULE"
fi

export OMP_NUM_THREADS=1
export SRUN_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK:-1}"

if [ $((GRID_SIZE % REGION_SIDE)) -ne 0 ]; then
  echo "PREDPREY_GRID_SIZE must be divisible by PREDPREY_REGION_SIDE"
  exit 1
fi

timestamp() {
  date "+%Y-%m-%d %H:%M:%S"
}

log_step() {
  echo "[$(timestamp)] $*"
}

extract_kv() {
  local key="$1"
  local file="$2"
  awk -v key="$key" '
  {
    for (i = 1; i <= NF; i++) {
      split($i, pair, "=")
      if (pair[1] == key) {
        print pair[2]
        exit
      }
    }
  }' "$file"
}

cat > "$CONFIG_TXT" <<EOF
source_dir=$SOURCE_DIR
run_dir=$RUN_DIR
runtime_source=$RUNTIME_SOURCE
model_source=$MODEL_SOURCE
cc=$CC_BIN
tasks_per_node=$TASKS_PER_NODE
seed_list=$SEED_LIST
rank_list=$RANK_LIST
grid_size=$GRID_SIZE
region_side=$REGION_SIDE
max_per_cell=$MAX_PER_CELL
initial_prey=$INITIAL_PREY
initial_predators=$INITIAL_PREDATORS
steps_per_day=$STEPS_PER_DAY
days=$DAYS
extra_cflags=$EXTRA_CFLAGS
validate_stride_days=$VALIDATE_STRIDE_DAYS
EOF

log_step "Compiling statistical validation binary"
"$CC_BIN" -O3 -std=c11 -Wall -Wextra -Wpedantic $EXTRA_CFLAGS \
  -DPP_GRID_SIZE="$GRID_SIZE" \
  -DPP_REGION_SIDE="$REGION_SIDE" \
  -DPP_MAX_PER_CELL="$MAX_PER_CELL" \
  -DPP_INITIAL_PREY="$INITIAL_PREY" \
  -DPP_INITIAL_PREDATORS="$INITIAL_PREDATORS" \
  -DPP_STEPS_PER_DAY="$STEPS_PER_DAY" \
  -DPP_DAYS="$DAYS" \
  -I"$SOURCE_DIR/framework" \
  "$RUNTIME_SOURCE" \
  "$MODEL_SOURCE" \
  -o "$TARGET"

printf "%s\n" \
  "ranks,seed,days_reported,final_day,final_prey,final_predators,mean_prey,mean_predators,peak_prey,peak_predators,extinct,total_run_seconds,avg_step_seconds,avg_day_seconds,move_seconds,occupancy_seconds,rng_seconds,allocation_seconds,occupancy_violations,duplicate_id_violations,migration_requests,migration_accepted,migration_denied,migration_balance" \
  > "$RUN_SUMMARY"

printf "%s\n" \
  "ranks,seed,day,prey,predators,total" \
  > "$TRAJECTORY_RAW"

run_case() {
  local ranks="$1"
  local seed="$2"
  local nodes_required=$(((ranks + TASKS_PER_NODE - 1) / TASKS_PER_NODE))
  local output_csv="$RESULTS_DIR/out_r${ranks}_seed${seed}.csv"
  local output_log="$RESULTS_DIR/out_r${ranks}_seed${seed}.log"
  local days_reported
  local final_day
  local final_prey
  local final_predators
  local mean_prey
  local mean_predators
  local peak_prey
  local peak_predators
  local extinct
  local total_run_seconds
  local avg_step_seconds
  local avg_day_seconds
  local move_seconds
  local occupancy_seconds
  local rng_seconds
  local allocation_seconds
  local occupancy_violations
  local duplicate_id_violations
  local migration_requests
  local migration_accepted
  local migration_denied
  local migration_balance

  log_step "Running statistical validation case ranks=$ranks seed=$seed"
  srun --exact \
    --nodes="$nodes_required" \
    --ntasks="$ranks" \
    --ntasks-per-node="$TASKS_PER_NODE" \
    --distribution=block:block \
    --hint=nomultithread \
    env PREDPREY_RUNTIME_SEED="$seed" PREDPREY_VALIDATE_STRIDE_DAYS="$VALIDATE_STRIDE_DAYS" "$TARGET" > "$output_csv" 2> "$output_log"

  read -r days_reported final_day final_prey final_predators mean_prey mean_predators peak_prey peak_predators <<EOF
$(awk -F, '
BEGIN {
  days = 0
  final_day = -1
  final_prey = 0
  final_predators = 0
  sum_prey = 0
  sum_predators = 0
  peak_prey = 0
  peak_predators = 0
}
NR > 1 && NF == 3 {
  days++
  final_day = $1
  final_prey = $2
  final_predators = $3
  sum_prey += $2
  sum_predators += $3
  if ($2 > peak_prey) peak_prey = $2
  if ($3 > peak_predators) peak_predators = $3
}
END {
  mean_prey = days > 0 ? sum_prey / days : 0
  mean_predators = days > 0 ? sum_predators / days : 0
  printf "%d %d %d %d %.6f %.6f %d %d\n", days, final_day, final_prey, final_predators, mean_prey, mean_predators, peak_prey, peak_predators
}' "$output_csv")
EOF

  extinct=0
  if grep -q "All animals extinct." "$output_csv"; then
    extinct=1
  fi

  awk -F, -v ranks="$ranks" -v seed="$seed" '
  NR > 1 && NF == 3 {
    printf "%s,%s,%s,%s,%s,%s\n", ranks, seed, $1, $2, $3, $2 + $3
  }' "$output_csv" >> "$TRAJECTORY_RAW"

  total_run_seconds="$(extract_kv total_run_seconds "$output_log")"
  avg_step_seconds="$(extract_kv avg_step_seconds "$output_log")"
  avg_day_seconds="$(extract_kv avg_day_seconds "$output_log")"
  move_seconds="$(extract_kv move_decision_seconds "$output_log")"
  occupancy_seconds="$(extract_kv occupancy_seconds "$output_log")"
  rng_seconds="$(extract_kv rng_seconds "$output_log")"
  allocation_seconds="$(extract_kv allocation_seconds "$output_log")"
  occupancy_violations="$(extract_kv occupancy_violations "$output_log")"
  duplicate_id_violations="$(extract_kv duplicate_id_violations "$output_log")"
  migration_requests="$(extract_kv migration_requests "$output_log")"
  migration_accepted="$(extract_kv migration_accepted "$output_log")"
  migration_denied="$(extract_kv migration_denied "$output_log")"
  migration_balance="$(extract_kv migration_balance "$output_log")"

  printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
    "$ranks" \
    "$seed" \
    "$days_reported" \
    "$final_day" \
    "$final_prey" \
    "$final_predators" \
    "$mean_prey" \
    "$mean_predators" \
    "$peak_prey" \
    "$peak_predators" \
    "$extinct" \
    "$total_run_seconds" \
    "$avg_step_seconds" \
    "$avg_day_seconds" \
    "$move_seconds" \
    "$occupancy_seconds" \
    "$rng_seconds" \
    "$allocation_seconds" \
    "$occupancy_violations" \
    "$duplicate_id_violations" \
    "$migration_requests" \
    "$migration_accepted" \
    "$migration_denied" \
    "$migration_balance" \
    >> "$RUN_SUMMARY"
}

for ranks in $RANK_LIST; do
  for seed in $SEED_LIST; do
    run_case "$ranks" "$seed"
  done
done

awk -F, '
BEGIN {
  OFS=","
  print "ranks,runs,mean_final_prey,min_final_prey,max_final_prey,mean_final_predators,min_final_predators,max_final_predators,mean_peak_prey,mean_peak_predators,mean_total_run_seconds,invariant_failures"
}
NR == 1 { next }
{
  rank = $1
  runs[rank]++
  sum_final_prey[rank] += $5
  sum_final_predators[rank] += $6
  sum_peak_prey[rank] += $9
  sum_peak_predators[rank] += $10
  sum_total_run[rank] += $12

  if (!(rank in min_final_prey) || $5 < min_final_prey[rank]) min_final_prey[rank] = $5
  if (!(rank in max_final_prey) || $5 > max_final_prey[rank]) max_final_prey[rank] = $5
  if (!(rank in min_final_predators) || $6 < min_final_predators[rank]) min_final_predators[rank] = $6
  if (!(rank in max_final_predators) || $6 > max_final_predators[rank]) max_final_predators[rank] = $6
  if ($24 != "PASS" || $19 != 0 || $20 != 0) invariant_failures[rank]++
}
END {
  for (rank in runs) {
    print rank,
          runs[rank],
          sum_final_prey[rank] / runs[rank],
          min_final_prey[rank],
          max_final_prey[rank],
          sum_final_predators[rank] / runs[rank],
          min_final_predators[rank],
          max_final_predators[rank],
          sum_peak_prey[rank] / runs[rank],
          sum_peak_predators[rank] / runs[rank],
          sum_total_run[rank] / runs[rank],
          invariant_failures[rank] + 0
  }
}' "$RUN_SUMMARY" | sort -t, -k1,1n > "$RANK_SUMMARY.tmp"

{
  head -n 1 "$RANK_SUMMARY.tmp"
  tail -n +2 "$RANK_SUMMARY.tmp"
} > "$RANK_SUMMARY"
rm -f "$RANK_SUMMARY.tmp"

awk -F, '
BEGIN {
  OFS=","
  print "ranks,day,runs,mean_prey,min_prey,max_prey,mean_predators,min_predators,max_predators"
}
NR == 1 { next }
{
  key = $1 OFS $3
  ranks[key] = $1
  day[key] = $3
  runs[key]++
  sum_prey[key] += $4
  sum_predators[key] += $5
  if (!(key in min_prey) || $4 < min_prey[key]) min_prey[key] = $4
  if (!(key in max_prey) || $4 > max_prey[key]) max_prey[key] = $4
  if (!(key in min_predators) || $5 < min_predators[key]) min_predators[key] = $5
  if (!(key in max_predators) || $5 > max_predators[key]) max_predators[key] = $5
}
END {
  for (key in runs) {
    print ranks[key],
          day[key],
          runs[key],
          sum_prey[key] / runs[key],
          min_prey[key],
          max_prey[key],
          sum_predators[key] / runs[key],
          min_predators[key],
          max_predators[key]
  }
}' "$TRAJECTORY_RAW" | sort -t, -k1,1n -k2,2n > "$TRAJECTORY_SUMMARY"

log_step "Statistical validation completed"
log_step "Run summary: $RUN_SUMMARY"
log_step "Rank summary: $RANK_SUMMARY"
log_step "Trajectory summary: $TRAJECTORY_SUMMARY"
log_step "Artifacts written to $RUN_DIR"
