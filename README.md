# Neuruh Workers

Python CLI workers for Notion-backed IP registry operations.

## Workers

- **Worker 1 — IP Logger** (`worker_1_ip_logger.py`): Files new IP assets to the Neuruh IP Registry with auto-generated Asset IDs.
- **Worker 2 — Build Status Updater** (`worker_2_build_status.py`): Flips Build State on an IP Registry entry by Asset ID.
- **Worker 3 — Context Loader** (`worker_3_context_loader.py`): Queries active (In Progress / Blocked / Needs Review) IP and writes a session brief to `~/neuruh/session-context.md`.

## Setup

```bash
git clone git@github.com:NeuruhAI/neuruh-workers.git ~/neuruh/workers
cd ~/neuruh/workers
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env with your Notion integration token + database IDs
```

## Get a Notion integration token

1. https://www.notion.so/profile/integrations → + New integration
2. Name it "Neuruh Workers", internal integration
3. Copy the token into `.env` as `NOTION_API_KEY`
4. Open each target database (IP Registry, Dev Log, Session Log) → ••• menu → Connections → add "Neuruh Workers"

## Usage

```bash
# File a new IP asset
python3 worker_1_ip_logger.py "My New Framework" AI FW "What it does in one sentence."

# Update build state
python3 worker_2_build_status.py AI-FW-017 "In Progress"

# Load session context
python3 worker_3_context_loader.py
cat ~/neuruh/session-context.md
```

## Shell alias

Add to your `~/.zshrc`:

```bash
alias ctx='cd ~/neuruh/workers && source .venv/bin/activate && python3 worker_3_context_loader.py && cat ~/neuruh/session-context.md'
```

Now `ctx` loads your session context from any terminal.

## License

MIT
