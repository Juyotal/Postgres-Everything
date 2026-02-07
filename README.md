# postgres_everything

> **One database. Six backends. Zero infrastructure.**

`postgres_everything` turns a single PostgreSQL instance into a complete
backend stack — replacing MongoDB, Redis, RabbitMQ, Elasticsearch, and
Pinecone with plain SQL and battle-tested Postgres extensions.

---

## What it replaces

| Traditional tool  | postgres_everything module | How                                          |
|-------------------|---------------------------|----------------------------------------------|
| MongoDB           | `DocumentStore`           | JSONB + GIN index + `@>` operator            |
| Redis (cache)     | `Cache`                   | Table with TTL column + lazy expiry          |
| RabbitMQ          | `TaskQueue`               | `FOR UPDATE SKIP LOCKED` + retry backoff     |
| Elasticsearch     | `SearchEngine`            | `tsvector` + `pg_trgm` + hybrid ranking      |
| Pinecone          | `VectorStore`             | `pgvector` + HNSW + SQL `WHERE` filters      |
| Redis Pub/Sub     | `PubSub`                  | `LISTEN` / `NOTIFY`                          |

---

## Quick start

```python
from postgres_everything import PostgresEverything

pg = PostgresEverything("postgresql://user:pass@localhost/mydb")
pg.init()   # run schema migrations (idempotent)

# Document store
pg.documents.insert("users", {"name": "Alice", "age": 30})
pg.documents.find("users", {"name": "Alice"})

# Cache
pg.cache.set("token:123", {"user_id": 42}, ttl=300)
pg.cache.get("token:123")

# Task queue
pg.queue.enqueue("send_email", {"to": "alice@example.com"})

# Full-text search
pg.search.index("PostgreSQL is awesome", "It handles nearly every backend need.")
pg.search.search("postgres backend")

# Vector search (pass a pre-computed vector or configure an EmbeddingProvider)
pg.vectors.add("pgvector is fast", embedding=[0.1, 0.2, ...])
pg.vectors.search([0.1, 0.2, ...], where={"user_id": "alice"})

# Pub/Sub
pg.pubsub.publish("events", {"type": "user.signup", "id": 1})

pg.close()
```

---

## Per-module usage

### DocumentStore

```python
# Insert
doc_id = pg.documents.insert("orders", {"item": "Widget", "qty": 3, "meta": {"sku": "W-1"}})

# Query with nested JSONB — uses GIN index automatically
results = pg.documents.find("orders", {"meta": {"sku": "W-1"}})

# Merge updates (JSONB || operator)
pg.documents.update("orders", {"item": "Widget"}, {"status": "shipped"})

# Count
pg.documents.count("orders", {"status": "shipped"})
```

### Cache

```python
pg.cache.set("session:abc", user_data, ttl=3600)
pg.cache.get("session:abc")
pg.cache.incr("page_views")
pg.cache.get_or_set("heavy_query", expensive_fn, ttl=60)
pg.cache.cleanup_expired()   # run periodically
```

### TaskQueue

```python
# Producer
pg.queue.enqueue("resize_image", {"image_id": 99, "width": 800}, priority=5)

# Consumer (register handlers then start worker)
@pg.queue.register("resize_image")
def resize_image(payload):
    ...

pg.queue.run_worker()        # blocking; SIGTERM/SIGINT for graceful stop
pg.queue.reap_stuck()        # reset zombie jobs (run on a cron)
pg.queue.stats()             # {"pending": 3, "running": 1, "completed": 42, "failed": 0}
```

### SearchEngine

```python
doc_id = pg.search.index("Postgres Performance", "Tuning tips for production", collection="blog")

# Full-text (stemming, ranking, snippets)
pg.search.search("performance tuning", collection="blog", snippet=True)

# Fuzzy / typo-tolerant
pg.search.fuzzy_search("Performence tuning", threshold=0.3)

# Hybrid (FTS + trigram combined score)
pg.search.hybrid_search("postgres tips")

# Autocomplete
pg.search.autocomplete("Post")
```

### VectorStore

```python
from postgres_everything.embeddings.openai import OpenAIEmbeddings

pg = PostgresEverything(DSN, embedding_provider=OpenAIEmbeddings())
pg.init()

# Add text — embedding generated automatically
pg.vectors.add("pgvector makes Postgres a vector DB", metadata={"user_id": "alice"})

# Semantic search + relational filter in ONE query (the key advantage)
pg.vectors.search("vector database", where={"user_id": "alice"}, since_days=30)

# Hybrid semantic + full-text
pg.vectors.hybrid_search("postgres vector search", semantic_weight=0.7)

# Build HNSW index for large-scale search (call once after first inserts)
pg.vectors.create_hnsw_index(dimensions=1536, distance="cosine")
```

### PubSub

```python
# Publisher
pg.pubsub.publish("notifications", {"event": "order.shipped", "order_id": 7})

# Subscriber (blocking; use a thread or process)
pg.pubsub.subscribe(["notifications"], callback=lambda ch, msg: print(ch, msg))

# Generator style
for channel, payload in pg.pubsub.listen(["notifications"]):
    process(channel, payload)
```

---

## Why PostgreSQL?

- **JSONB** stores parsed binary JSON; the GIN index makes `@>` (contains)
  queries instant even on deeply nested fields.
- **`FOR UPDATE SKIP LOCKED`** lets multiple workers pick jobs from a shared
  table without blocking or deadlocking — lock-free job dispatch.
- **`tsvector`** strips stop words and stems tokens ("running" → "run") so
  full-text queries work on word roots, not exact strings.  `pg_trgm` breaks
  words into 3-char chunks for fuzzy matching despite typos.
- **pgvector** + HNSW = approximate nearest-neighbour search in milliseconds.
  The critical advantage: vector `<=>` distance lives _inside_ PostgreSQL, so
  you can combine it with ordinary SQL `WHERE` filters in a single round-trip.
- **`LISTEN`/`NOTIFY`** is built-in pub/sub delivered over the existing
  connection — no extra broker process.

---

## Setup

```bash
# 1. Start Postgres 16 with all extensions pre-loaded
docker compose up -d

# 2. Install
pip install -e ".[all]"   # includes openai, sentence-transformers, pytest, ruff, mypy

# 3. Configure
cp .env.example .env
```

The Docker image (`pgvector/pgvector:pg16`) ships with `vector`, `pg_trgm`,
`unaccent`, `uuid-ossp`, and `pgcrypto` pre-installed.

---

## Production notes

- **Pooling**: The default pool size is 10.  Tune `pool_size` on
  `PostgresEverything` based on your workload and `max_connections` on the
  Postgres server.
- **Migrations**: `pg.init()` is idempotent — safe to call at every app
  startup.  It tracks applied migrations in `pg_everything_migrations`.
- **Worker scaling**: Run multiple `task_worker.py` processes; `SKIP LOCKED`
  guarantees each job is processed exactly once.
- **Monitoring**: Query `pg_jobs` grouped by status to build dashboards.
  Call `queue.reap_stuck()` on a cron to recover from crashed workers.
- **HNSW index**: For large vector collections, call
  `vectors.create_hnsw_index(dimensions=N)` once.  Without it, Postgres falls
  back to an exact sequential scan (correct but slower at scale).
- **Cache cleanup**: Run `cache.cleanup_expired()` periodically (e.g. every
  hour) to reclaim disk space from expired entries.

---

## License

MIT
