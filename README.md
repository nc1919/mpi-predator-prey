# Parallel actor-based predator-prey simulation

An MPI experiment that applies actor-style message passing to a spatial predator-prey model. The repository contains a serial reference, a baseline MPI actor runtime, a lower-overhead actor runtime, Slurm experiment drivers, and validation variants.

## Repository layout

- `single_process/` — serial C reference implementation.
- `predator_prey/framework/` — MPI actor runtimes.
- `predator_prey/model/` — baseline, fast, and validation model variants.
- `predator_prey/*.slurm` — smoke, scaling, and validation jobs.
- [RESULTS.md](RESULTS.md) — curated results and important limitations.

## Build locally

A C11 compiler and an MPI implementation are required.

```bash
mpicc -O3 -std=c11 -Wall -Wextra -Wpedantic \
  -Ipredator_prey/framework \
  predator_prey/framework/actor_runtime.c \
  predator_prey/model/predator_prey.c \
  -o predprey_actor

mpirun -n 4 ./predprey_actor
```

For the lower-overhead implementation, replace `actor_runtime.c` and `predator_prey.c` with `actor_model_fast.c` and `predator_prey_fast.c`.

## Run on Slurm

The job files derive source and run directories from their own location and no longer contain a personal account or filesystem path. Cluster module names and allocation directives are still site-specific; review them before submission.

```bash
sbatch predator_prey/pred_prey.slurm
sbatch predator_prey/pred_prey_experiment.slurm
sbatch predator_prey/pred_prey_validate_statistical.slurm
```

Environment variables prefixed with `PREDPREY_` override workload size, rank counts, compiler modules, and output locations.

## Status and limitations

The scaling measurements are preliminary single runs. Archived validation showed repeatability for a fixed MPI rank count, but the MPI trajectory did not match the serial reference; statistical invariant checks passed for the sampled seeds and ranks. See [RESULTS.md](RESULTS.md) before interpreting performance numbers as evidence of correctness.

This began as coursework/experimental HPC code. Confirm any institutional sharing requirements and contributor permissions before changing repository visibility.

## License

No project-wide licence has been selected. Until the copyright holder adds one, the code is available for review but no reuse rights are granted.
