"""Tiny CPU-only Slurm payload; writes exclusively a fresh Job-ID directory."""
import getpass
import json
import os
import socket
from datetime import datetime, timezone
from pathlib import Path

root = Path(os.environ["SLURM_SUBMIT_DIR"])
job_id = os.environ["SLURM_JOB_ID"]
assert job_id.isdecimal()
assert root.is_absolute() and root.resolve() == Path.cwd().resolve()
assert os.environ["SLURM_CPUS_PER_TASK"] == "1"
assert not os.environ.get("SLURM_JOB_GPUS")
output = root / "output"
assert not output.is_symlink()
output.mkdir(exist_ok=True)
destination = output / job_id
destination.mkdir()  # Refuse overwriting an earlier result.
result = {
    "job_id": job_id,
    "hostname": socket.gethostname(),
    "user": getpass.getuser(),
    "utc": datetime.now(timezone.utc).isoformat(),
    "partition": os.environ["SLURM_JOB_PARTITION"],
    "cpus_per_task": int(os.environ["SLURM_CPUS_PER_TASK"]),
    "ntasks": int(os.environ["SLURM_NTASKS"]),
    "gpu_requested": False,
    "sum_squares_1_to_100": sum(x * x for x in range(1, 101)),
}
text = json.dumps(result, sort_keys=True)
with (destination / "result.json").open("x", encoding="utf-8") as stream:
    stream.write(text + "\n")
print(text, flush=True)
