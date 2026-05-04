# Banking Knowledge Assistant

A RAG system for working with banking regulations and internal CIB documents. This repository contains not just a chat application, but the full corpus-preparation stack: parsing real regulations, generating synthetic internal documents, generating QA datasets, chunking, indexing, the online retrieval/API layer, an observability dashboard, and offline evaluation.

## What This Project Does

The system solves two connected tasks:

1. Builds a domain corpus for a banking assistant.
2. Answers employee questions over that corpus using hybrid RAG.

In its current state, the repository already contains prepared artifacts:

- `17` parsed real regulations in `data/parsed/`
- `18` generated internal regulations in `data/generated/`
- `18` question sets in `data/questions/`
- `35` chunk files in `data/chunks/`
- cluster BM25 indexes in `data/bm25_indexes/`
- local Chroma state in `data/chroma_db/`

## Key Capabilities

- Parsing real regulatory documents from external sources with cache, fallback sources, and manual PDF/ODT override.
- Generating synthetic internal bank regulations based on the regulatory base defined in `public/config.yaml`.
- Generating question-answer datasets for downstream retrieval and answer-quality evaluation.
- Chunking both real regulations and generated documents.
- Hybrid retrieval: semantic search in ChromaDB + BM25 + reciprocal rank fusion.
- Streaming chat via FastAPI and SSE.
- React/Vite web interface with a dedicated dashboard route.
- Logging chat, retrieval, and pipeline runs into PostgreSQL.
- Offline evaluation for retrieval/classifier/NER/answer scenarios.

## Application Diagram

![Application workflow diagram](public/images/app-workflow-diagram.png)

## Architecture

### 1. Data Preparation Flow

1. `src/data_scripts/parsing/parsing.py`
   - reads `real_documents[]` from `public/config.yaml`
   - downloads and extracts real regulations
   - saves sections into `data/parsed/{doc_id}_sections.json` and `..._sections_tree.json`
2. `src/data_scripts/generating/generating.py`
   - reads `documents[]` from `public/config.yaml`
   - pulls required sections from `data/parsed/`
   - generates synthetic internal banking regulations into `data/generated/`
3. `src/data_scripts/generating/questions.py`
   - builds QA sets from `data/generated/*.md`
   - saves them into `data/questions/`
4. `src/data_scripts/generating/validator.py`
   - validates generated documents and question sets
   - writes a report to `data/validation_report.json`
5. `src/data_scripts/chunking/chunking.py`
   - splits real and synthetic documents into chunks
   - writes `data/chunks/{doc_id}_chunks.json`
6. `src/data_scripts/indexing/indexing.py`
   - builds embeddings
   - sends chunks into ChromaDB
   - marks dirty clusters for BM25 rebuild
7. `src/data_scripts/indexing/bm25_rebuild.py`
   - rebuilds BM25 indexes for changed clusters

### 2. Runtime Flow

1. A user submits a question in the React client.
2. `client/src/api/chat.ts` sends POST to `/api/chat` and reads an SSE stream.
3. FastAPI route `src/api/routers/chat.py`:
   - gets conversation history
   - calls hybrid retrieval
   - passes retrieved chunks into the LLM
   - streams response tokens back to the UI
4. `src/retriever/__init__.py` performs:
   - query embedding
   - semantic search in ChromaDB
   - BM25 search over cluster indexes
   - fusion via RRF
5. `src/llm/__init__.py` sends context and history to DeepSeek through an OpenAI-compatible SDK.
6. The answer and sources are returned to the chat UI.

### 3. Observability Flow

- Chat and retrieval events are written to PostgreSQL.
- `src/api/routers/dashboard.py` aggregates online metrics and artifact statistics from `data/`.
- `client/src/pages/DashboardPage.tsx` shows:
  - chat KPIs
  - retrieval KPIs
  - parse/gen pipeline sessions
  - validation summary
  - artifact statistics
- `src/evaluation/runner.py` runs offline evaluation and refreshes the HTML dashboard at `data/eval/dashboard.html`.

## Technology Stack

### Backend

- Python
- FastAPI
- Uvicorn
- Pydantic Settings
- OpenAI SDK for a DeepSeek-compatible API

### Retrieval / ML

- ChromaDB
- BM25 (`rank-bm25`)
- `sentence-transformers`
- `transformers`
- `pymorphy2`

### Frontend

- React 18
- Vite
- TypeScript
- Tailwind CSS
- Recharts

### Infrastructure

- PostgreSQL 16
- Docker Compose

## Repository Structure

```text
.
├── client/                    # React/Vite frontend
├── data/
│   ├── parsed/                # parsed real regulations
│   ├── generated/             # synthetic internal regulations
│   ├── questions/             # QA sets for generated docs
│   ├── chunks/                # retrieval chunks
│   ├── bm25_indexes/          # BM25 indexes by cluster
│   ├── chroma_db/             # local indexing state / Chroma data
│   ├── fetch_cache/           # downloaded source cache
│   ├── eval/                  # offline evaluation runs and dashboard
│   └── manual_pdfs/           # manual PDF/ODT fallback for parsing
├── public/
│   ├── config.yaml            # main corpus and generation pipeline config
│   ├── images/                # README/doc images
│   └── *.md                   # internal engineering guides for pipelines
├── src/
│   ├── api/                   # FastAPI app, routes, models, services
│   ├── llm/                   # streaming client for DeepSeek
│   ├── retriever/             # hybrid retrieval and DB logging
│   ├── data_scripts/          # parsing/chunking/indexing/generating pipelines
│   └── evaluation/            # offline evaluation framework
├── docker-compose.yml         # PostgreSQL + Chroma
├── requirements.txt
└── README.md
```

## Environment Variables

The project reads settings from `.env`. Below is the minimal set of variables actually used by the code:

```env
DEEPSEEK_API_KEY=
DEEPSEEK_BASE_URL=https://api.deepseek.com
DEEPSEEK_MODEL=deepseek-chat

POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=banking_assistant
POSTGRES_USER=banking_user
POSTGRES_PASSWORD=changeme

CHROMA_HOST=localhost
CHROMA_PORT=8000

GARANT_LOGIN=
GARANT_PASSWORD=
```

Notes:

- `DEEPSEEK_API_KEY` is required for the generation pipeline, question generation, and runtime chat.
- `GARANT_LOGIN` / `GARANT_PASSWORD` are only needed if you want to automate access to closed Garant documents through Playwright.
- FastAPI also reads retrieval parameters from `src/api/core/config.py`: `history_window`, `retrieval_top_k`, `retrieval_candidates`, `retrieval_rrf_k`, `bm25_dir`, and observability version tags.

## Quick Start

### 1. Start infrastructure

```bash
docker compose up -d
```

This starts:

- PostgreSQL on `${POSTGRES_PORT:-5433}`
- ChromaDB on `${CHROMA_PORT:-8000}`

### 2. Install Python dependencies

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

If you need parsing for closed documents through Playwright:

```bash
pip install playwright
playwright install chromium
```

### 3. Start the backend

```bash
uvicorn src.api.main:app --reload --port 8000
```

Healthcheck:

```bash
curl http://127.0.0.1:8000/health
```

### 4. Start the frontend

```bash
cd client
npm install
npm run dev
```

By default, Vite runs on `http://127.0.0.1:5173` and proxies `/api` to `http://127.0.0.1:8000`.

### 5. Open the interfaces

- Chat: `http://127.0.0.1:5173/`
- Dashboard: `http://127.0.0.1:5173/dashboard`
- API docs: `http://127.0.0.1:8000/docs`

## Typical Corpus Preparation Order

If you are deploying the project without relying on the already prepared artifacts, the usual order is:

### 1. Parse real regulations

```bash
.venv\Scripts\python.exe src/data_scripts/parsing/parsing.py --log-level INFO
```

Useful modes:

```bash
.venv\Scripts\python.exe src/data_scripts/parsing/parsing.py --force
.venv\Scripts\python.exe src/data_scripts/parsing/parsing.py --only 115-FZ 590-P
.venv\Scripts\python.exe src/data_scripts/parsing/parsing.py --fetch-force
```

Parsing fallback modes:

- if a source is closed, place `data/manual_pdfs/{doc_id}.odt` or `.pdf`
- if `GARANT_LOGIN` / `GARANT_PASSWORD` are set, the Playwright downloader for Garant is used

### 2. Generate synthetic regulations

```bash
.venv\Scripts\python.exe src/data_scripts/generating/generating.py --log-level INFO
```

Useful modes:

```bash
.venv\Scripts\python.exe src/data_scripts/generating/generating.py --force
.venv\Scripts\python.exe src/data_scripts/generating/generating.py --only RG-KIB-001 RG-KIB-003
```

### 3. Generate questions

```bash
.venv\Scripts\python.exe src/data_scripts/generating/questions.py --log-level INFO
```

Examples:

```bash
.venv\Scripts\python.exe src/data_scripts/generating/questions.py --force
.venv\Scripts\python.exe src/data_scripts/generating/questions.py --only RG-KIB-001
.venv\Scripts\python.exe src/data_scripts/generating/questions.py --count 15
```

### 4. Validate generated artifacts

```bash
.venv\Scripts\python.exe src/data_scripts/generating/validator.py --check all --log-level INFO
```

### 5. Chunk documents

```bash
.venv\Scripts\python.exe src/data_scripts/chunking/chunking.py --log-level INFO
```

Examples:

```bash
.venv\Scripts\python.exe src/data_scripts/chunking/chunking.py --force
.venv\Scripts\python.exe src/data_scripts/chunking/chunking.py --source real
.venv\Scripts\python.exe src/data_scripts/chunking/chunking.py --source synthetic
```

### 6. Index into Chroma

```bash
.venv\Scripts\python.exe src/data_scripts/indexing/indexing.py --log-level INFO
```

Examples:

```bash
.venv\Scripts\python.exe src/data_scripts/indexing/indexing.py --force
.venv\Scripts\python.exe src/data_scripts/indexing/indexing.py --only 115-FZ 590-P
.venv\Scripts\python.exe src/data_scripts/indexing/indexing.py --cluster compliance
```

### 7. Rebuild BM25

```bash
.venv\Scripts\python.exe src/data_scripts/indexing/bm25_rebuild.py --dirty-only
```

## Runtime API

### `POST /api/chat`

Input:

```json
{
  "session_id": "uuid",
  "message": "What are the KYC requirements for a high-risk client?"
}
```

The output is an SSE stream with event types:

- `sources` — list of retrieved sources
- `delta` — streamed LLM response chunks
- `done` — final answer and service `_meta`

### `GET /health`

Returns:

```json
{"status": "ok"}
```

### Dashboard endpoints

Main groups:

- `/api/dashboard/overview`
- `/api/dashboard/chat/*`
- `/api/dashboard/retrieve/*`
- `/api/dashboard/llm/*`
- `/api/dashboard/artifacts`
- `/api/dashboard/pipeline`

If PostgreSQL is unavailable, the dashboard routes are implemented to return empty structures where possible instead of breaking the entire API.

## Frontend

Frontend routes:

- `/` — chat window
- `/dashboard` — operational dashboard

Chat behavior:

- the client keeps a `session_id` via `uuid`
- messages are rendered progressively as `delta` SSE events arrive
- sources are shown separately as soon as the backend sends `sources`

The dashboard shows four areas:

- chat
- retrieve
- parsing
- generation

## Evaluation

The `src/evaluation/` module provides a unified evaluation runner.

Supported suites:

- `retrieval`
- `classifier`
- `ner`
- `answer`
- `full`

### Run examples

Retrieval only:

```bash
.venv\Scripts\python.exe -m src.evaluation.runner --suite retrieval --top-k 5
```

Full run:

```bash
.venv\Scripts\python.exe -m src.evaluation.runner --suite full --top-k 5
```

Write results into a separate directory:

```bash
.venv\Scripts\python.exe -m src.evaluation.runner --suite retrieval --output-root data/eval/runs
```

Artifacts produced:

- `data/eval/runs/<run_id>/report.json`
- `data/eval/runs/<run_id>/cases.jsonl`
- `data/eval/runs/index.jsonl`
- `data/eval/dashboard.html`

Datasets are stored in `src/evaluation/datasets/`.

## Logging and Observability

The project writes several log streams:

- chat requests / LLM calls / retrieved chunks into PostgreSQL
- parse sessions and parse operations into PostgreSQL
- generating/questions/validator sessions into PostgreSQL
- retrieval requests into PostgreSQL
- offline evaluation runs into `data/eval/`

Operationally, this gives you:

- diagnostics for slow and stuck chats
- retrieval quality analysis
- visibility into pipeline completeness
- overview of artifact sizes and quality in `data/`

## Best Entry Points in the Codebase

If you need a fast way into the codebase, start here:

- `public/config.yaml` — domain model of the bank, real/synthetic document lists, and generation order
- `src/api/app/main.py` — FastAPI application assembly
- `src/api/routers/chat.py` — runtime chat flow
- `src/retriever/__init__.py` — hybrid retrieval
- `src/llm/__init__.py` — SSE streaming and DeepSeek client
- `src/data_scripts/parsing/parsing.py` — parsing real regulations
- `src/data_scripts/generating/generating.py` — generating synthetic docs
- `src/data_scripts/generating/questions.py` — generating QA
- `src/data_scripts/chunking/chunking.py` — chunk preparation
- `src/data_scripts/indexing/indexing.py` — Chroma indexing
- `src/api/routers/dashboard.py` — analytics backend and aggregations
- `client/src/pages/DashboardPage.tsx` — dashboard frontend
- `src/evaluation/runner.py` — unified evaluation entrypoint

## Limitations and Notes

- Chat runtime depends on availability of the DeepSeek-compatible API.
- Retrieval runtime depends on a running Chroma instance and existing BM25 indexes.
- Some parsing scenarios depend on external websites and their markup stability.
- For closed Garant sources, automation only works with valid credentials and a functioning Playwright setup.
- The repository contains engineering guides in `public/*.md`; they are useful for maintenance, but they do not replace reading the code.

## License

See `LICENSE`.
