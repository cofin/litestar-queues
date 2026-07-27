# Live Cloud Tasks topology proof

`test_live_topology.py` proves the scale-to-zero topology against a real Google
project: a record enqueued by one process is held by Cloud Tasks, delivered to a
cold private Cloud Run instance, and comes back terminal in shared storage.

It is release evidence, run by hand. It is not a CI gate, and it skips itself
unless you explicitly ask for it. Everything CI can prove is already proved in
`src/tests/unit/execution/cloudtasks/` against an injected client — what is left
here is the part that needs Google, because there is no local Cloud Tasks API.

## What it costs and what it touches

- It creates Cloud Tasks deliveries on the queue you name, and deletes exactly
  the ones it created when the run ends, including when the run fails.
- It provisions nothing. The queue, the service, and the IAM bindings are yours,
  and it never lists, sweeps, or modifies anything it did not create.
- It writes and settles queue records in the database your configuration names.
  **Point it at a disposable project and a disposable database.**
- A full run is a few dozen task creations plus a few Cloud Run requests, and
  spends roughly three minutes waiting on deliberately delayed deliveries.

## What you need first

1. **A Cloud Tasks queue** in your project and region.
2. **A private Cloud Run service** running your consumer, deployed with no
   minimum instances, a request timeout above the task timeout, and no public
   invoker.
3. **A service account** with `roles/run.invoker` on that service, named as the
   Cloud Tasks OIDC identity.
4. **A database both processes reach**, with the queue schema applied.
5. **The probe tasks registered in the consumer.** Copy `probe_tasks.py` from
   this directory into your consumer image and point
   `LITESTAR_QUEUES_TASK_MODULES` at it. A consumer that does not have these
   names registered will retire every record as an unknown task instead of
   running it, and every case here will fail.
6. **One configuration both processes share** — a module-level factory returning
   the `QueueConfig` that carries your queue backend and your
   `CloudTasksExecutionConfig`. The consumer already resolves its configuration
   this way; the proof resolves the same one, which is what makes it a proof
   rather than two guesses that happen to agree.

## Running it

```bash
export LITESTAR_QUEUES_GCP_LIVE=1
export LITESTAR_QUEUES_CONFIG_FACTORY="your_app.queue:config"
export LITESTAR_QUEUES_GCP_EVIDENCE_PATH="$PWD/cloud-tasks-evidence.json"   # optional
export LITESTAR_QUEUES_GCP_TIMEOUT=180                                      # optional

uv run pytest src/tests/integration/execution/cloudtasks/test_live_topology.py -q
```

Application-default credentials are discovered by the Google client on first
use. Nothing here reads or stores a credential, and the gate is decided from the
two environment variables above before your configuration is imported at all —
so a run you did not ask for never authenticates.

Without `LITESTAR_QUEUES_GCP_LIVE=1` the whole module skips with the reason
printed. That is the honest result to report when the resources are unavailable;
do not substitute the unit tier's results for it.

## What it records

Timing evidence is written locally as JSON to `LITESTAR_QUEUES_GCP_EVIDENCE_PATH`,
or under pytest's temporary directory if you do not set one. It carries cold and
warm delivery durations stamped with the region and the Python runtime, and the
consumer-side completion timestamp. Nothing is uploaded anywhere.

## The cases

| Case | What it proves |
| --- | --- |
| cold delivery | An enqueue with no worker anywhere still runs, on an instance that was scaled to zero |
| cold then warm | The cold-start cost, measured rather than assumed |
| delayed delivery | A future record is held by Google and cannot be claimed early, because nothing polls |
| duplicate delivery | A second delivery for one record leaves exactly one claim owner |
| failing task | The consumer creates its own retry delivery, and the record settles after it |
| cancelled record | Storage's decision outlives a delivery Google is already holding |
| missing delivery | One bounded repair pass re-creates a delivery the transport lost |
| already-exists | The real Google error is the one the package's structural match recognizes |

## Cleaning up

Cleanup is automatic and best-effort: each delivery is deleted by the exact name
Google returned, and one that is already gone — the expected case, since most of
them are meant to be dispatched — is not an error. If the run is killed outright,
the deliveries it created are the ones prefixed `lq-` on your queue, and they
expire on your queue's own retention policy.
