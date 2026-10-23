#!/bin/bash
#SBATCH --job-name=hpc-mcp-cancel-smoke
#SBATCH --partition=debug
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=128M
#SBATCH --time=00:02:00
#SBATCH --output=logs/cancel-smoke-%j.out
#SBATCH --error=logs/cancel-smoke-%j.err
set -euo pipefail
printf 'job_id=%s\nhostname=%s\nuser=%s\nstarted=%s\n' \
    "${SLURM_JOB_ID:?}" "$(hostname)" "$(whoami)" "$(date -u +%FT%TZ)"
sleep 90
printf 'completed_without_cancellation\n'
