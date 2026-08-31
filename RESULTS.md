# Results

## Baseline MPI scaling experiment

Recorded workload: 40×40 grid, 5×5 regions, 1,600 initial prey, 400 initial predators, 40 steps/day for 20 days. Each point is one run, so no uncertainty estimate is available.

| MPI ranks | Elapsed (s) | Speedup | Parallel efficiency |
|---:|---:|---:|---:|
| 1 | 726.30 | 1.00× | 100.0% |
| 2 | 373.11 | 1.95× | 97.3% |
| 4 | 196.72 | 3.69× | 92.3% |
| 8 | 102.68 | 7.07× | 88.4% |
| 16 | 92.97 | 7.81× | 48.8% |

The efficiency drop at 16 ranks suggests the workload was no longer large enough to amortise communication and coordination overhead.

## Validation evidence

- Deterministic-repeat jobs produced stable output for repeated runs at the same rank count.
- Those MPI outputs did **not** match the serial reference trajectory. This is an unresolved correctness limitation, not a rounding claim.
- A separate statistical validation covered ranks 1, 2, and 4 with three seeds per rank. It reported no occupancy, duplicate-ID, or migration-balance invariant failures in those nine sampled runs.

Raw scheduler logs, binaries, and generated run directories were removed from the review tree. Re-run the checked-in scripts to regenerate evidence with machine, compiler, MPI, repeat-count, and uncertainty metadata before using these figures in a report.
