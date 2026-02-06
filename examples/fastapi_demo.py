"""FastAPI demo — one endpoint per postgres_everything module.

Start with:
    uvicorn examples.fastapi_demo:app --reload

Requires:
    pip install fastapi uvicorn
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from postgres_everything import PostgresEverything

DSN = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pg_everything")
pg: PostgresEverything


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pg
    pg = PostgresEverything(DSN)
    pg.init()
    yield
    pg.close()


app = FastAPI(title="postgres_everything demo", lifespan=lifespan)


# ---------------------------------------------------------------------------
# DocumentStore
# ---------------------------------------------------------------------------


class DocumentIn(BaseModel):
    data: dict[str, Any]


@app.post("/documents/{collection}", status_code=201)
def create_document(collection: str, body: DocumentIn):
    doc_id = pg.documents.insert(collection, body.data)
    return {"id": doc_id}


@app.get("/documents/{collection}")
def list_documents(collection: str, limit: int = Query(20, le=100)):
    return pg.documents.find(collection, limit=limit)


@app.get("/documents/{collection}/{doc_id}")
def get_document(collection: str, doc_id: str):
    doc = pg.documents.find_by_id(doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Document not found")
    return doc


# ---------------------------------------------------------------------------
# SearchEngine
# ---------------------------------------------------------------------------


class SearchDocIn(BaseModel):
    title: str
    body: str = ""
    metadata: dict[str, Any] = {}


@app.post("/search/index", status_code=201)
def index_search_doc(collection: str = "default", body: SearchDocIn = ...):
    doc_id = pg.search.index(body.title, body.body, collection=collection, metadata=body.metadata)
    return {"id": doc_id}


@app.get("/search")
def search(
    q: str = Query(...),
    collection: str | None = None,
    fuzzy: bool = False,
    limit: int = Query(10, le=50),
):
    if fuzzy:
        return pg.search.fuzzy_search(q, collection=collection, limit=limit)
    return pg.search.search(q, collection=collection, limit=limit)


@app.get("/search/autocomplete")
def autocomplete(prefix: str = Query(...), collection: str | None = None, limit: int = 5):
    return pg.search.autocomplete(prefix, collection=collection, limit=limit)


# ---------------------------------------------------------------------------
# VectorStore
# ---------------------------------------------------------------------------


class VectorIn(BaseModel):
    content: str
    metadata: dict[str, Any] = {}
    collection: str = "default"
    embedding: list[float] | None = None


@app.post("/vectors", status_code=201)
def add_vector(body: VectorIn):
    if body.embedding is None:
        raise HTTPException(
            status_code=400,
            detail="Provide an 'embedding' vector (no provider configured in demo).",
        )
    v_id = pg.vectors.add(
        body.content,
        collection=body.collection,
        metadata=body.metadata,
        embedding=body.embedding,
    )
    return {"id": v_id}


@app.get("/vectors/search")
def vector_search(
    q: list[float] = Query(...),
    collection: str | None = None,
    user_id: str | None = None,
    limit: int = Query(10, le=50),
):
    where = {"user_id": user_id} if user_id else None
    return pg.vectors.search(q, collection=collection, where=where, limit=limit)


# ---------------------------------------------------------------------------
# TaskQueue
# ---------------------------------------------------------------------------


class JobIn(BaseModel):
    task_name: str
    payload: dict[str, Any] = {}
    queue: str = "default"
    priority: int = 0
    delay_seconds: int = 0


@app.post("/jobs", status_code=201)
def enqueue_job(body: JobIn):
    job_id = pg.queue.enqueue(
        body.task_name,
        body.payload,
        queue=body.queue,
        priority=body.priority,
        delay_seconds=body.delay_seconds,
    )
    return {"id": job_id}


@app.get("/jobs/stats")
def job_stats(queue: str | None = None):
    return pg.queue.stats(queue=queue)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------


class CacheIn(BaseModel):
    value: Any
    ttl: int | None = None


@app.get("/cache/{key}")
def get_cache(key: str):
    value = pg.cache.get(key)
    if value is None:
        raise HTTPException(status_code=404, detail="Key not found or expired")
    return {"key": key, "value": value}


@app.put("/cache/{key}", status_code=204)
def set_cache(key: str, body: CacheIn):
    pg.cache.set(key, body.value, ttl=body.ttl)


@app.delete("/cache/{key}", status_code=204)
def delete_cache(key: str):
    pg.cache.delete(key)
