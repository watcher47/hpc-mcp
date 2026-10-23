#!/bin/bash
#SBATCH --job-name=hpc-cpu-smoke
#SBATCH --partition=debug
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=128M
#SBATCH --time=00:02:00
#SBATCH --output=logs/cpu-smoke-%j.out
#SBATCH --error=logs/cpu-smoke-%j.err
set -euo pipefail
cd -- "${SLURM_SUBMIT_DIR:?}"
python3 smoke.py
