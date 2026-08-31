#!/bin/bash
#SBATCH --job-name=predprey_fast_experiment
#SBATCH --partition=standard
#SBATCH --qos=standard
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=16
#SBATCH --cpus-per-task=1
#SBATCH --time=04:00:00
#SBATCH --output=%x-%j.out
#SBATCH --error=%x-%j.err

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="${PREDPREY_SOURCE_DIR:-$SCRIPT_DIR}"
RUN_BASE="${PREDPREY_RUN_BASE:-$SCRIPT_DIR/.runs}"
RUN_DIR="${RUN_BASE%/}/predprey_fast_experiment_${SLURM_JOB_ID}"
BUILD_DIR="$RUN_DIR/build"
RESULTS_DIR="$RUN_DIR/results"

RUNTIME_SOURCE="${PREDPREY_RUNTIME_SOURCE:-$SOURCE_DIR/framework/actor_model_fast.c}"
MODEL_SOURCE="${PREDPREY_MODEL_SOURCE:-$SOURCE_DIR/model/predator_prey_fast.c}"
TARGET_NAME="${PREDPREY_TARGET_NAME:-predprey_actor_fast_experiment}"

COMPILER_MODULE="${PREDPREY_COMPILER_MODULE:-PrgEnv-gnu}"
MPI_MODULE="${PREDPREY_MPI_MODULE:-cray-mpich}"
CPU_MODULE="${PREDPREY_CPU_MODULE:-craype-x86-rome}"
CC_BIN="${PREDPREY_CC:-cc}"

ALLOC_NODES="${SLURM_JOB_NUM_NODES:-4}"
ALLOC_TASKS_PER_NODE_RAW="${SLURM_TASKS_PER_NODE:-16}"
ALLOC_TASKS_PER_NODE_RAW="${ALLOC_TASKS_PER_NODE_RAW%%(*}"
ALLOC_TASKS_PER_NODE_RAW="${ALLOC_TASKS_PER_NODE_RAW%%,*}"
TASKS_PER_NODE="${PREDPREY_TASKS_PER_NODE:-$ALLOC_TASKS_PER_NODE_RAW}"
REPEATS="${PREDPREY_REPEATS:-3}"
RANK_LIST="${PREDPREY_RANK_LIST:-1 2 4 8 16 32 64}"

EXP_GRID_SIZE="${PREDPREY_EXP_GRID_SIZE:-120}"
EXP_REGION_SIDE="${PREDPREY_EXP_REGION_SIDE:-5}"
EXP_MAX_PER_CELL="${PREDPREY_EXP_MAX_PER_CELL:-100}"
EXP_INITIAL_PREY="${PREDPREY_EXP_INITIAL_PREY:-14400}"
EXP_INITIAL_PREDATORS="${PREDPREY_EXP_INITIAL_PREDATORS:-3600}"
EXP_STEPS_PER_DAY="${PREDPREY_EXP_STEPS_PER_DAY:-200}"
EXP_DAYS="${PREDPREY_EXP_DAYS:-120}"

EXTRA_CFLAGS="${PREDPREY_EXTRA_CFLAGS:-}"
CFLAGS="-O3 -std=c11 -Wall -Wextra -Wpedantic"
TARGET="$BUILD_DIR/$TARGET_NAME"
SUMMARY_RAW="$RESULTS_DIR/timings_raw.csv"
SUMMARY_AVG="$RESULTS_DIR/timings_avg.csv"
SCALING_CSV="$RESULTS_DIR/scaling.csv"
CONFIG_TXT="$RESULTS_DIR/experiment_config.txt"

mkdir -p "$BUILD_DIR" "$RESULTS_DIR"

if [ ! -d "$SOURCE_DIR" ]; then
  echo "Source directory does not exist: $SOURCE_DIR"
  exit 1
fi

if [ ! -f "$RUNTIME_SOURCE" ]; then
  echo "Runtime source does not exist: $RUNTIME_SOURCE"
  exit 1
fi

if [ ! -f "$MODEL_SOURCE" ]; then
  echo "Model source does not exist: $MODEL_SOURCE"
  exit 1
fi

if [ $((EXP_GRID_SIZE % EXP_REGION_SIDE)) -ne 0 ]; then
  echo "PREDPREY_EXP_GRID_SIZE must be divisible by PREDPREY_EXP_REGION_SIDE"
  exit 1
fi

REGIONS_PER_DIM=$((EXP_GRID_SIZE / EXP_REGION_SIDE))
REGION_COUNT=$((REGIONS_PER_DIM * REGIONS_PER_DIM))
MAX_ALLOC_RANKS=$((ALLOC_NODES * TASKS_PER_NODE))
MAX_RANK=0

read -r -a SCALE_RANKS <<< "$RANK_LIST"
for ranks in "${SCALE_RANKS[@]}"; do
  if [ "$ranks" -gt "$MAX_RANK" ]; then
    MAX_RANK="$ranks"
  fi
done

if [ "$MAX_RANK" -gt "$MAX_ALLOC_RANKS" ]; then
  echo "Requested max rank count $MAX_RANK exceeds allocation capacity $MAX_ALLOC_RANKS"
  exit 1
fi

if [ "$MAX_RANK" -gt "$REGION_COUNT" ]; then
  echo "Requested max rank count $MAX_RANK exceeds region count $REGION_COUNT"
  echo "Increase PREDPREY_EXP_GRID_SIZE or reduce PREDPREY_RANK_LIST"
  exit 1
fi

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

export CC="$CC_BIN"
export OMP_NUM_THREADS=1
export SRUN_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK:-1}"

cat > "$CONFIG_TXT" <<EOF
source_dir=$SOURCE_DIR
run_dir=$RUN_DIR
compiler_module=$COMPILER_MODULE
mpi_module=$MPI_MODULE
cpu_module=$CPU_MODULE
cc=$CC_BIN
tasks_per_node=$TASKS_PER_NODE
repeats=$REPEATS
rank_list=$RANK_LIST
runtime_source=$RUNTIME_SOURCE
model_source=$MODEL_SOURCE
target_name=$TARGET_NAME
grid_size=$EXP_GRID_SIZE
region_side=$EXP_REGION_SIDE
region_count=$REGION_COUNT
max_per_cell=$EXP_MAX_PER_CELL
initial_prey=$EXP_INITIAL_PREY
initial_predators=$EXP_INITIAL_PREDATORS
steps_per_day=$EXP_STEPS_PER_DAY
days=$EXP_DAYS
extra_cflags=$EXTRA_CFLAGS
EOF

echo "Source dir: $SOURCE_DIR"
echo "Run dir: $RUN_DIR"
echo "Compiling fast experiment binary..."
$CC $CFLAGS $EXTRA_CFLAGS \
  -DPP_GRID_SIZE="$EXP_GRID_SIZE" \
  -DPP_REGION_SIDE="$EXP_REGION_SIDE" \
  -DPP_MAX_PER_CELL="$EXP_MAX_PER_CELL" \
  -DPP_INITIAL_PREY="$EXP_INITIAL_PREY" \
  -DPP_INITIAL_PREDATORS="$EXP_INITIAL_PREDATORS" \
  -DPP_STEPS_PER_DAY="$EXP_STEPS_PER_DAY" \
  -DPP_DAYS="$EXP_DAYS" \
  -I"$SOURCE_DIR/framework" \
  "$RUNTIME_SOURCE" \
  "$MODEL_SOURCE" \
  -o "$TARGET"

printf "%s\n" \
  "ranks,nodes,tasks_per_node,repeat,grid_size,region_side,initial_prey,initial_predators,steps_per_day,days,elapsed_seconds,output_csv" \
  > "$SUMMARY_RAW"

run_case() {
  local ranks="$1"
  local repeat="$2"
  local nodes_required=$(((ranks + TASKS_PER_NODE - 1) / TASKS_PER_NODE))
  local output_csv="$RESULTS_DIR/out_r${ranks}_rep${repeat}.csv"
  local time_file="$RESULTS_DIR/time_r${ranks}_rep${repeat}.txt"
  local host_file="$RESULTS_DIR/hosts_r${ranks}_rep${repeat}.txt"
  local host_count
  local elapsed

  echo "Running fast case ranks=$ranks repeat=$repeat nodes=$nodes_required"
  srun --exact \
    --nodes="$nodes_required" \
    --ntasks="$ranks" \
    --ntasks-per-node="$TASKS_PER_NODE" \
    --distribution=block:block \
    --hint=nomultithread \
    -l /bin/hostname > "$host_file"

  host_count="$(awk '{print $2}' "$host_file" | sort -u | wc -l | tr -d ' ')"
  if [ "$host_count" -lt "$nodes_required" ]; then
    echo "Expected at least $nodes_required distinct hosts for ranks=$ranks, got $host_count"
    exit 1
  fi

  /usr/bin/time -f "%e" -o "$time_file" \
    srun --exact \
      --nodes="$nodes_required" \
      --ntasks="$ranks" \
      --ntasks-per-node="$TASKS_PER_NODE" \
      --distribution=block:block \
      --hint=nomultithread \
      "$TARGET" > "$output_csv"

  elapsed="$(cat "$time_file")"
  printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
    "$ranks" \
    "$nodes_required" \
    "$TASKS_PER_NODE" \
    "$repeat" \
    "$EXP_GRID_SIZE" \
    "$EXP_REGION_SIDE" \
    "$EXP_INITIAL_PREY" \
    "$EXP_INITIAL_PREDATORS" \
    "$EXP_STEPS_PER_DAY" \
    "$EXP_DAYS" \
    "$elapsed" \
    "$(basename "$output_csv")" \
    >> "$SUMMARY_RAW"
}

for ranks in "${SCALE_RANKS[@]}"; do
  for repeat in $(seq 1 "$REPEATS"); do
    run_case "$ranks" "$repeat"
  done
done

awk -F, '
BEGIN {
  OFS=","
  print "ranks,nodes,tasks_per_node,grid_size,region_side,initial_prey,initial_predators,steps_per_day,days,avg_elapsed_seconds,min_elapsed_seconds,max_elapsed_seconds,repeats"
}
NR == 1 {
  next
}
{
  key = $1 OFS $2 OFS $3 OFS $5 OFS $6 OFS $7 OFS $8 OFS $9 OFS $10
  sum[key] += $11
  count[key] += 1
  if (!(key in min) || $11 < min[key]) {
    min[key] = $11
  }
  if (!(key in max) || $11 > max[key]) {
    max[key] = $11
  }
}
END {
  for (key in sum) {
    print key, sum[key] / count[key], min[key], max[key], count[key]
  }
}
' "$SUMMARY_RAW" > "$SUMMARY_AVG.tmp"

{
  head -n 1 "$SUMMARY_AVG.tmp"
  tail -n +2 "$SUMMARY_AVG.tmp" | sort -t, -k1,1n
} > "$SUMMARY_AVG"
rm -f "$SUMMARY_AVG.tmp"

awk -F, '
BEGIN {
  OFS=","
  print "ranks,nodes,tasks_per_node,avg_elapsed_seconds,min_elapsed_seconds,max_elapsed_seconds,repeats,speedup,efficiency"
}
NR == 1 {
  next
}
NR == 2 {
  baseline = $10
}
{
  speedup = baseline / $10
  efficiency = speedup / $1
  print $1, $2, $3, $10, $11, $12, $13, speedup, efficiency
}
' "$SUMMARY_AVG" > "$SCALING_CSV"

echo "Fast experiment completed"
echo "Raw timings: $SUMMARY_RAW"
echo "Averaged timings: $SUMMARY_AVG"
echo "Scaling summary: $SCALING_CSV"
echo "Artifacts written to $RUN_DIR"
