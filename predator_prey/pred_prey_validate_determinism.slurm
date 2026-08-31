#!/bin/bash
#SBATCH --job-name=predprey_validate
#SBATCH --partition=standard
#SBATCH --qos=standard
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1
#SBATCH --time=00:20:00
#SBATCH --output=%x-%j.out
#SBATCH --error=%x-%j.err

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="${PREDPREY_SOURCE_DIR:-$SCRIPT_DIR}"
RUN_BASE="${PREDPREY_RUN_BASE:-$SCRIPT_DIR/.runs}"
RUN_DIR="${RUN_BASE%/}/predprey_validate_${SLURM_JOB_ID}"
BUILD_DIR="$RUN_DIR/build"
RESULTS_DIR="$RUN_DIR/results"

SERIAL_SOURCE="${PREDPREY_SERIAL_SOURCE:-$SOURCE_DIR/../single_process/predator_prey.c}"
MPI_RUNTIME_SOURCE="${PREDPREY_MPI_RUNTIME_SOURCE:-$SOURCE_DIR/framework/actor_runtime.c}"
MPI_MODEL_SOURCE="${PREDPREY_MPI_MODEL_SOURCE:-$SOURCE_DIR/model/predator_prey.c}"

COMPILER_MODULE="${PREDPREY_COMPILER_MODULE:-PrgEnv-gnu}"
MPI_MODULE="${PREDPREY_MPI_MODULE:-cray-mpich}"
CPU_MODULE="${PREDPREY_CPU_MODULE:-craype-x86-rome}"
SERIAL_CC_BIN="${PREDPREY_SERIAL_CC:-cc}"
MPI_CC_BIN="${PREDPREY_MPI_CC:-cc}"
ALLOC_TASKS_PER_NODE_RAW="${SLURM_TASKS_PER_NODE:-8}"
ALLOC_TASKS_PER_NODE_RAW="${ALLOC_TASKS_PER_NODE_RAW%%(*}"
ALLOC_TASKS_PER_NODE_RAW="${ALLOC_TASKS_PER_NODE_RAW%%,*}"
TASKS_PER_NODE="${PREDPREY_TASKS_PER_NODE:-$ALLOC_TASKS_PER_NODE_RAW}"

SEED="${PREDPREY_SEED:-123456}"
GRID_SIZE="${PREDPREY_GRID_SIZE:-20}"
REGION_SIDE="${PREDPREY_REGION_SIDE:-5}"
MAX_PER_CELL="${PREDPREY_MAX_PER_CELL:-100}"
INITIAL_PREY="${PREDPREY_INITIAL_PREY:-400}"
INITIAL_PREDATORS="${PREDPREY_INITIAL_PREDATORS:-100}"
STEPS_PER_DAY="${PREDPREY_STEPS_PER_DAY:-20}"
DAYS="${PREDPREY_DAYS:-10}"
DETERMINISTIC_RUNS="${PREDPREY_DETERMINISTIC_RUNS:-2}"
RANK_LIST="${PREDPREY_RANK_LIST:-1 2 4 8 16}"
EXTRA_CFLAGS="${PREDPREY_EXTRA_CFLAGS:-}"

SERIAL_BIN="$BUILD_DIR/predprey_serial_validation"
MPI_BIN="$BUILD_DIR/predprey_mpi_validation"
SUMMARY_CSV="$RESULTS_DIR/validation_summary.csv"
CONFIG_TXT="$RESULTS_DIR/validation_config.txt"

mkdir -p "$BUILD_DIR" "$RESULTS_DIR"

if [ ! -d "$SOURCE_DIR" ]; then
  echo "Source directory does not exist: $SOURCE_DIR"
  exit 1
fi

if [ ! -f "$SERIAL_SOURCE" ]; then
  echo "Serial source not found: $SERIAL_SOURCE"
  exit 1
fi

if [ ! -f "$MPI_RUNTIME_SOURCE" ]; then
  echo "MPI runtime source not found: $MPI_RUNTIME_SOURCE"
  exit 1
fi

if [ ! -f "$MPI_MODEL_SOURCE" ]; then
  echo "MPI model source not found: $MPI_MODEL_SOURCE"
  exit 1
fi

if [ $((GRID_SIZE % REGION_SIDE)) -ne 0 ]; then
  echo "PREDPREY_GRID_SIZE must be divisible by PREDPREY_REGION_SIDE"
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

export OMP_NUM_THREADS=1
export SRUN_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK:-1}"

timestamp() {
  date "+%Y-%m-%d %H:%M:%S"
}

log_step() {
  echo "[$(timestamp)] $*"
}

record_check() {
  local implementation="$1"
  local ranks="$2"
  local run_index="$3"
  local check_name="$4"
  local status="$5"
  local reference_csv="$6"
  local output_csv="$7"
  local details="$8"

  printf "%s,%s,%s,%s,%s,%s,%s,%s\n" \
    "$implementation" \
    "$ranks" \
    "$run_index" \
    "$check_name" \
    "$status" \
    "$reference_csv" \
    "$output_csv" \
    "$details" \
    >> "$SUMMARY_CSV"
}

compare_or_record_failure() {
  local implementation="$1"
  local ranks="$2"
  local run_index="$3"
  local check_name="$4"
  local reference_csv="$5"
  local output_csv="$6"
  local details_ok="$7"
  local details_fail="$8"

  if cmp -s "$reference_csv" "$output_csv"; then
    record_check "$implementation" "$ranks" "$run_index" "$check_name" "PASS" \
      "$(basename "$reference_csv")" "$(basename "$output_csv")" "$details_ok"
    return
  fi

  local diff_file="$RESULTS_DIR/${implementation}_r${ranks}_run${run_index}_${check_name}.diff"
  diff -u "$reference_csv" "$output_csv" > "$diff_file" || true
  record_check "$implementation" "$ranks" "$run_index" "$check_name" "FAIL" \
    "$(basename "$reference_csv")" "$(basename "$output_csv")" "$details_fail:$(basename "$diff_file")"
}

run_mpi_case() {
  local ranks="$1"
  local output_csv="$2"
  local nodes_required=$(((ranks + TASKS_PER_NODE - 1) / TASKS_PER_NODE))

  log_step "Starting MPI run: ranks=$ranks nodes=$nodes_required output=$(basename "$output_csv")"
  srun --exact \
    --nodes="$nodes_required" \
    --ntasks="$ranks" \
    --ntasks-per-node="$TASKS_PER_NODE" \
    --distribution=block:block \
    --hint=nomultithread \
    "$MPI_BIN" > "$output_csv"
  log_step "Finished MPI run: ranks=$ranks output=$(basename "$output_csv")"
}

cat > "$CONFIG_TXT" <<EOF
source_dir=$SOURCE_DIR
run_dir=$RUN_DIR
build_dir=$BUILD_DIR
results_dir=$RESULTS_DIR
serial_source=$SERIAL_SOURCE
mpi_runtime_source=$MPI_RUNTIME_SOURCE
mpi_model_source=$MPI_MODEL_SOURCE
serial_cc=$SERIAL_CC_BIN
mpi_cc=$MPI_CC_BIN
seed=$SEED
grid_size=$GRID_SIZE
region_side=$REGION_SIDE
max_per_cell=$MAX_PER_CELL
initial_prey=$INITIAL_PREY
initial_predators=$INITIAL_PREDATORS
steps_per_day=$STEPS_PER_DAY
days=$DAYS
deterministic_runs=$DETERMINISTIC_RUNS
rank_list=$RANK_LIST
extra_cflags=$EXTRA_CFLAGS
EOF

COMMON_FLAGS=(
  -O3
  -std=c11
  -Wall
  -Wextra
  -Wpedantic
  $EXTRA_CFLAGS
)

SERIAL_DEFINES=(
  -DPP_SEED="$SEED"
  -DGRID_SIZE="$GRID_SIZE"
  -DMAX_PER_CELL="$MAX_PER_CELL"
  -DINITIAL_PREY="$INITIAL_PREY"
  -DINITIAL_PREDATORS="$INITIAL_PREDATORS"
  -DSTEPS_PER_DAY="$STEPS_PER_DAY"
  -DDAYS="$DAYS"
)

MPI_DEFINES=(
  -DPP_SEED="$SEED"
  -DPP_GRID_SIZE="$GRID_SIZE"
  -DPP_REGION_SIDE="$REGION_SIDE"
  -DPP_MAX_PER_CELL="$MAX_PER_CELL"
  -DPP_INITIAL_PREY="$INITIAL_PREY"
  -DPP_INITIAL_PREDATORS="$INITIAL_PREDATORS"
  -DPP_STEPS_PER_DAY="$STEPS_PER_DAY"
  -DPP_DAYS="$DAYS"
)

log_step "Run dir: $RUN_DIR"
log_step "Validation workload: grid=$GRID_SIZE region_side=$REGION_SIDE prey=$INITIAL_PREY predators=$INITIAL_PREDATORS steps_per_day=$STEPS_PER_DAY days=$DAYS seed=$SEED"

log_step "Building serial validation binary"
"$SERIAL_CC_BIN" "${COMMON_FLAGS[@]}" "${SERIAL_DEFINES[@]}" "$SERIAL_SOURCE" -o "$SERIAL_BIN"

log_step "Building MPI validation binary"
"$MPI_CC_BIN" "${COMMON_FLAGS[@]}" "${MPI_DEFINES[@]}" -I"$SOURCE_DIR/framework" \
  "$MPI_RUNTIME_SOURCE" \
  "$MPI_MODEL_SOURCE" \
  -o "$MPI_BIN"

printf "%s\n" \
  "implementation,ranks,run_index,check,status,reference_csv,output_csv,details" \
  > "$SUMMARY_CSV"

SERIAL_BASELINE="$RESULTS_DIR/serial_baseline_run1.csv"
log_step "Starting serial baseline run 1 output=$(basename "$SERIAL_BASELINE")"
"$SERIAL_BIN" > "$SERIAL_BASELINE"
log_step "Finished serial baseline run 1 output=$(basename "$SERIAL_BASELINE")"
record_check "serial" "1" "1" "baseline_generated" "PASS" "" "$(basename "$SERIAL_BASELINE")" "serial baseline captured"

serial_run=2
while [ "$serial_run" -le "$DETERMINISTIC_RUNS" ]; do
  serial_output="$RESULTS_DIR/serial_baseline_run${serial_run}.csv"
  log_step "Starting serial repeat run $serial_run output=$(basename "$serial_output")"
  "$SERIAL_BIN" > "$serial_output"
  log_step "Finished serial repeat run $serial_run output=$(basename "$serial_output")"
  compare_or_record_failure \
    "serial" "1" "$serial_run" "deterministic_repeat" \
    "$SERIAL_BASELINE" "$serial_output" \
    "serial repeat matched run1" "serial repeat diverged from run1"
  serial_run=$((serial_run + 1))
done

MPI_RANK1_REFERENCE=""
for ranks in $RANK_LIST; do
  run_index=1
  rank_reference="$RESULTS_DIR/mpi_r${ranks}_run1.csv"
  run_mpi_case "$ranks" "$rank_reference"
  record_check "mpi" "$ranks" "$run_index" "baseline_generated" "PASS" "" "$(basename "$rank_reference")" "MPI baseline captured"

  if [ "$ranks" = "1" ]; then
    MPI_RANK1_REFERENCE="$rank_reference"
  fi

  compare_or_record_failure \
    "mpi" "$ranks" "$run_index" "matches_serial_baseline" \
    "$SERIAL_BASELINE" "$rank_reference" \
    "MPI output matched serial baseline" "MPI output diverged from serial baseline"

  if [ -n "$MPI_RANK1_REFERENCE" ] && [ "$ranks" != "1" ]; then
    compare_or_record_failure \
      "mpi" "$ranks" "$run_index" "matches_rank1" \
      "$MPI_RANK1_REFERENCE" "$rank_reference" \
      "MPI output matched rank1 reference" "MPI output diverged from rank1 reference"
  fi

  run_index=2
  while [ "$run_index" -le "$DETERMINISTIC_RUNS" ]; do
    output_csv="$RESULTS_DIR/mpi_r${ranks}_run${run_index}.csv"
    run_mpi_case "$ranks" "$output_csv"
    compare_or_record_failure \
      "mpi" "$ranks" "$run_index" "deterministic_repeat" \
      "$rank_reference" "$output_csv" \
      "MPI repeat matched run1" "MPI repeat diverged from run1"

    compare_or_record_failure \
      "mpi" "$ranks" "$run_index" "matches_serial_baseline" \
      "$SERIAL_BASELINE" "$output_csv" \
      "MPI output matched serial baseline" "MPI output diverged from serial baseline"

    if [ -n "$MPI_RANK1_REFERENCE" ] && [ "$ranks" != "1" ]; then
      compare_or_record_failure \
        "mpi" "$ranks" "$run_index" "matches_rank1" \
        "$MPI_RANK1_REFERENCE" "$output_csv" \
        "MPI output matched rank1 reference" "MPI output diverged from rank1 reference"
    fi

    run_index=$((run_index + 1))
  done
done

log_step "Validation completed"
log_step "Summary: $SUMMARY_CSV"
log_step "Config: $CONFIG_TXT"
log_step "Artifacts written to $RUN_DIR"
