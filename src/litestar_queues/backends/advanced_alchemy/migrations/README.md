# Advanced Alchemy Queue Backend Migrations

These Alembic migration assets create the `litestar_queue_tasks` table used by
`AdvancedAlchemyQueueBackend`. Applications own their Advanced Alchemy
`SQLAlchemyPlugin` and Alembic environment; reference this packaged migration
location from application migration tooling when queue persistence should be
managed by Alembic instead of `create_schema=True`.
