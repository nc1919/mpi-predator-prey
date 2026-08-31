#!/bin/bash
#SBATCH --job-name=predprey_smoke
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
RUN_DIR="${RUN_BASE%/}/predprey_run_${SLURM_JOB_ID}"
BUILD_DIR="$RUN_DIR/build"
RESULTS_DIR="$RUN_DIR/results"
RUNTIME_SOURCE="${PREDPREY_RUNTIME_SOURCE:-$SOURCE_DIR/framework/actor_runtime.c}"
MODEL_SOURCE="${PREDPREY_MODEL_SOURCE:-$SOURCE_DIR/model/predator_prey.c}"
TARGET_NAME="${PREDPREY_TARGET_NAME:-predprey_actor}"
SKIP_OUTPUT_COMPARE="${PREDPREY_SKIP_OUTPUT_COMPARE:-0}"

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
echo "Compiling..."
$CC $CFLAGS \
  -I"$SOURCE_DIR/framework" \
  "$RUNTIME_SOURCE" \
  "$MODEL_SOURCE" \
  -o "$TARGET"

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

echo "Running 1-rank reference on a single node"
srun --exact \
  --nodes=1 \
  --ntasks=1 \
  --distribution=block:block \
  --hint=nomultithread \
  "$TARGET" > "$RESULTS_DIR/out.1.csv"
echo "  completed 1-rank reference"

echo "Running 2-rank cross-node check"
srun --exact \
  --nodes=2 \
  --ntasks=2 \
  --ntasks-per-node=1 \
  --distribution=block:block \
  --hint=nomultithread \
  "$TARGET" > "$RESULTS_DIR/out.2nodes.csv"
echo "  completed 2-rank cross-node case"

echo "Comparing 2-node output against the 1-rank reference"
if [ "$SKIP_OUTPUT_COMPARE" = "1" ]; then
  echo "Skipping output comparison by request"
elif ! cmp -s "$RESULTS_DIR/out.1.csv" "$RESULTS_DIR/out.2nodes.csv"; then
  echo "Output mismatch between 1 rank and 2 ranks across 2 nodes"
  diff -u "$RESULTS_DIR/out.1.csv" "$RESULTS_DIR/out.2nodes.csv" | sed -n '1,80p'
  exit 1
fi

echo "Verification passed"
echo "Reference output:"
sed -n '1,12p' "$RESULTS_DIR/out.1.csv"
echo "Artifacts written to $RUN_DIR"
