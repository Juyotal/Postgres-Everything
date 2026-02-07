# Examples

## Setup

```bash
# 1. Start Postgres with all required extensions
docker compose up -d

# 2. Install the library and all optional dependencies
pip install -e ".[all]"

# 3. Copy the env file and fill in your values
cp .env.example .env
# Edit DATABASE_URL (and OPENAI_API_KEY if using OpenAI embeddings)
```

## Running the examples

### FastAPI demo

Demonstrates all six modules via HTTP endpoints.

```bash
pip install fastapi uvicorn
uvicorn examples.fastapi_demo:app --reload
# Visit http://localhost:8000/docs
```

### Task worker

A standalone worker process that processes jobs from the default queue.

```bash
python examples/task_worker.py
```

To enqueue a job for the worker to process:

```python
from postgres_everything import PostgresEverything
pg = PostgresEverything("postgresql://postgres:postgres@localhost:5432/pg_everything")
pg.init(modules=["queue"])
pg.queue.enqueue("send_email", {"to": "alice@example.com", "subject": "Hello"})
pg.close()
```

### RAG app

A minimal retrieval-augmented generation demo combining VectorStore and Cache.

```bash
# With toy (offline) embeddings — no API key required
python examples/rag_app.py --demo

# With OpenAI embeddings
OPENAI_API_KEY=sk-... python examples/rag_app.py --query "How does pgvector work?"

# Filter results by user
python examples/rag_app.py --demo --user bob
```
