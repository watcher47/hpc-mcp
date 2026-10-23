# CPU workflow fixture

This fixture belongs to the existing hpc-mcp repository. Register this directory
as the MCP local project; never synchronize the repository or another research
project for this smoke test. Use only a verified existing server mapping.

The regular workflow uploads only smoke.py, slurm/cpu_smoke.sbatch and logs/.gitkeep. The
batch requests one task, one CPU, 128 MiB and two minutes, no GPU, in the
controller-verified debug partition. Only submit under an explicit CPU workflow
test request. Do not change shared runtimes, other jobs, credentials or services.

Results are output/<JobID>/result.json and logs/cpu-smoke-<JobID>.{out,err} on
the remote project. Download via project-scoped MCP to
collected/<request-UUID>/ and verify transfer SHA-256 and size before accepting.
Preserve prior results. Store the durable UUID, receipt and tool evidence in the
repository's ignored .superpowers directory. Only exact main-job COMPLETED and
0:0 plus the expected identity, resources and calculation pass acceptance.

For an explicitly authorized cancellation capability test, separately upload
slurm/cancel_smoke.sbatch through remote_upload; .hpcignore deliberately keeps it
out of the regular workflow's three-file manifest. This isolated test requests
the same CPU/memory/time limits, prints identity and sleeps for 90 seconds. Submit
once with a durable request UUID, then cancel only the returned new JobID through
slurm_cancel_job with its own durable UUID. Verify exact main-job CANCELLED and
accounting exit code, capture both logs and audit evidence. Never substitute a
research job, a previously cancelled job, or another user's job. Do not attach this brief test to an unrelated
scientific task or restart the monitoring agent.
