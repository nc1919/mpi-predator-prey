#!/bin/bash
#SBATCH --job-name=predprey_fast_smoke
#SBATCH --partition=standard
#SBATCH --qos=standard
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --time=00:05:00
#SBATCH --output=%x-%j.out
#SBATCH --error=%x-%j.err

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="${PREDPREY_SOURCE_DIR:-$SCRIPT_DIR}"
RUN_BASE="${PREDPREY_RUN_BASE:-$SCRIPT_DIR/.runs}"
RUN_DIR="${RUN_BASE%/}/predprey_fast_run_${SLURM_JOB_ID}"
BUILD_DIR="$RUN_DIR/build"
RESULTS_DIR="$RUN_DIR/results"

RUNTIME_SOURCE="${PREDPREY_RUNTIME_SOURCE:-$SOURCE_DIR/framework/actor_model_fast.c}"
MODEL_SOURCE="${PREDPREY_MODEL_SOURCE:-$SOURCE_DIR/model/predator_prey_fast.c}"
TARGET_NAME="${PREDPREY_TARGET_NAME:-predprey_actor_fast}"
SKIP_OUTPUT_COMPARE="${PREDPREY_SKIP_OUTPUT_COMPARE:-0}"

SMOKE_GRID_SIZE="${PREDPREY_SMOKE_GRID_SIZE:-20}"
SMOKE_REGION_SIDE="${PREDPREY_SMOKE_REGION_SIDE:-5}"
SMOKE_MAX_PER_CELL="${PREDPREY_SMOKE_MAX_PER_CELL:-100}"
SMOKE_INITIAL_PREY="${PREDPREY_SMOKE_INITIAL_PREY:-400}"
SMOKE_INITIAL_PREDATORS="${PREDPREY_SMOKE_INITIAL_PREDATORS:-100}"
SMOKE_STEPS_PER_DAY="${PREDPREY_SMOKE_STEPS_PER_DAY:-50}"
SMOKE_DAYS="${PREDPREY_SMOKE_DAYS:-20}"
EXTRA_CFLAGS="${PREDPREY_EXTRA_CFLAGS:-}"

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

module -q restore >/dev/null 2>&1 || true
module -q load PrgEnv-gnu
module -q load cray-mpich
module -q load craype-x86-rome

export CC=cc
export OMP_NUM_THREADS=1
export SRUN_CPUS_PER_TASK="$SLURM_CPUS_PER_TASK"

CFLAGS="-O3 -std=c11 -Wall -Wextra -Wpedantic"
TARGET="$BUILD_DIR/$TARGET_NAME"

echo "Source dir: $SOURCE_DIR"
echo "Run dir: $RUN_DIR"
echo "Compiling fast implementation..."
$CC $CFLAGS $EXTRA_CFLAGS \
  -DPP_GRID_SIZE="$SMOKE_GRID_SIZE" \
  -DPP_REGION_SIDE="$SMOKE_REGION_SIDE" \
  -DPP_MAX_PER_CELL="$SMOKE_MAX_PER_CELL" \
  -DPP_INITIAL_PREY="$SMOKE_INITIAL_PREY" \
  -DPP_INITIAL_PREDATORS="$SMOKE_INITIAL_PREDATORS" \
  -DPP_STEPS_PER_DAY="$SMOKE_STEPS_PER_DAY" \
  -DPP_DAYS="$SMOKE_DAYS" \
  -I"$SOURCE_DIR/framework" \
  "$RUNTIME_SOURCE" \
  "$MODEL_SOURCE" \
  -o "$TARGET"

echo "Smoke parameters:"
echo "  grid_size=$SMOKE_GRID_SIZE"
echo "  region_side=$SMOKE_REGION_SIDE"
echo "  max_per_cell=$SMOKE_MAX_PER_CELL"
echo "  initial_prey=$SMOKE_INITIAL_PREY"
echo "  initial_predators=$SMOKE_INITIAL_PREDATORS"
echo "  steps_per_day=$SMOKE_STEPS_PER_DAY"
echo "  days=$SMOKE_DAYS"

echo "Checking that Slurm placed ranks on two hosts"
srun --exact \
  --nodes=2 \
  --ntasks=2 \
  --ntasks-per-node=1 \
  --distribution=block:block \
  --hint=nomultithread \
  -l /bin/hostname | tee "$RESULTS_DIR/hosts.txt"

host_count="$(awk '{print $2}' "$RESULTS_DIR/hosts.txt" | sort -u | wc -l | tr -d ' ')"
if [ "$host_count" -lt 2 ]; then
  echo "Expected at least 2 distinct hosts for the placement check, got $host_count"
  exit 1
fi

echo "Running 1-rank fast reference on a single node"
srun --exact \
  --nodes=1 \
  --ntasks=1 \
  --distribution=block:block \
  --hint=nomultithread \
  "$TARGET" > "$RESULTS_DIR/out.1.csv"
if [ ! -s "$RESULTS_DIR/out.1.csv" ]; then
  echo "1-rank fast run completed but produced no output: $RESULTS_DIR/out.1.csv"
  exit 1
fi
echo "  completed 1-rank fast reference"

echo "Running 2-rank fast cross-node check"
srun --exact \
  --nodes=2 \
  --ntasks=2 \
  --ntasks-per-node=1 \
  --distribution=block:block \
  --hint=nomultithread \
  "$TARGET" > "$RESULTS_DIR/out.2nodes.csv"
if [ ! -s "$RESULTS_DIR/out.2nodes.csv" ]; then
  echo "2-rank fast cross-node run completed but produced no output: $RESULTS_DIR/out.2nodes.csv"
  exit 1
fi
echo "  completed 2-rank fast cross-node case"

echo "Comparing 2-node output against the 1-rank reference"
if [ "$SKIP_OUTPUT_COMPARE" = "1" ]; then
  echo "Skipping output comparison by request"
elif ! cmp -s "$RESULTS_DIR/out.1.csv" "$RESULTS_DIR/out.2nodes.csv"; then
  echo "Fast output mismatch between 1 rank and 2 ranks across 2 nodes"
  diff -u "$RESULTS_DIR/out.1.csv" "$RESULTS_DIR/out.2nodes.csv" | sed -n '1,80p'
  exit 1
fi

echo "Fast smoke run completed"
echo "1-rank output:"
sed -n '1,12p' "$RESULTS_DIR/out.1.csv"
echo "2-rank output:"
sed -n '1,12p' "$RESULTS_DIR/out.2nodes.csv"
echo "Artifacts written to $RUN_DIR"
