---
name: matrix-indexer
description: Search indexed Matrix messages using the matrix-indexer-search CLI tool on donghouse. Query historical Matrix chat history from the MongoDB-backed indexer.
---

# Matrix Indexer Search

Search indexed Matrix messages using the `matrix-indexer-search` CLI tool.

## Usage

```bash
matrix-indexer-search "<text>" [--limit N] [--room ROOM_ID] [--sender SENDER] [--from TIME] [--to TIME]
```

## Options

| Option | Description |
|--------|-------------|
| `--limit N` | Max rows to return (default: 20) |
| `--room ROOM_ID` | Restrict to a Matrix room_id |
| `--sender SENDER` | Restrict to sender mxid |
| `--from TIME` | Lower time bound (ISO-8601 or unix ms) |
| `--to TIME` | Upper time bound (ISO-8601 or unix ms) |
| `--case-sensitive` | Case-sensitive regex match |
| `--include-redacted` | Include redacted messages |

## Examples

```bash
# Search for recent mentions of "scoob"
matrix-indexer-search "scoob" --limit 50

# Search specific room
matrix-indexer-search "keyword" --room "!CUqbYAuoIkIOvzXnCA:cclub.cs.wmich.edu"

# Search date range
matrix-indexer-search " deploy " --from 2026-03-01 --to 2026-04-01

# Search from specific sender
matrix-indexer-search "donghouse" --sender "@slimeq:cclub.cs.wmich.edu"
```

## Notes

- Redacted messages omitted by default
- Text matched as regex (case-insensitive by default)
- Database: `matrix_index` on MongoDB (donghouse:27017)
- Tool: `/usr/local/bin/matrix-indexer-search`
- Repo: `patrick-slimelab/matrix-indexer.NET`
- Run on: donghouse (the Matrix indexer host)