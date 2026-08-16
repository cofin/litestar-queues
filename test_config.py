from litestar_queues import QueueConfig, WorkerConfig

config = QueueConfig(
    worker=WorkerConfig(
        placement="external",
        heartbeat_miss_threshold=1,
        cancel_on_claim_loss=True,
        max_concurrency=1
    ),
    queue_backend="memory"
)
print("Config worker threshold:", config.worker.heartbeat_miss_threshold)
