---
name: Matrix Indexer Search
description: Search indexed Matrix messages using the `matrix-indexer-search` CLI tool. Supports regex, date ranges, room/sender filtering, and more.
---

## Usage

```bash
matrix-indexer-search "<text>" [--limit N] [--room ROOM_ID] [--sender SENDER] [--from TIME] [--to TIME]
```

## Options

| Option | Description |
|--------|-------------|
| `--limit N` | Max rows to return (default: 20) |
| `--room ROOM_ID` | Restrict to a Matrix room_id (e.g., `!abc123:example.com`) |
| `--sender SENDER` | Restrict to sender mxid (e.g., `@user:example.com`) |
| `--from TIME` | Lower time bound (ISO-8601 or unix ms) |
| `--to TIME` | Upper time bound (ISO-8601 or unix ms) |
| `--case-sensitive` | Case-sensitive regex match |
| `--include-redacted` | Include redacted messages |

## Examples

```bash
# Basic keyword search
matrix-indexer-search "keyword" --limit 50

# Search specific room
matrix-indexer-search "bot" --room "!roomId:example.com"

# Search date range
matrix-indexer-search " deploy " --from 2026-03-01 --to 2026-04-01

# Search from specific sender
matrix-indexer-search "announcement" --sender "@admin:example.com"

# Regex search (find URLs)
matrix-indexer-search "https?://[^ ]+" --limit 30

# Combine filters: messages from a user in a specific timeframe
matrix-indexer-search "meeting" --sender "@user:example.com" --from 2026-01-01 --to 2026-04-01
```

## Search Tips

1. **Start broad, then refine**: Use a general term first, then add filters
2. **Regex is powerful**: Search for patterns like URLs, emails, version numbers
3. **Use date ranges**: Narrow results with `--from`/`--to` for large result sets
4. **Room IDs over aliases**: Room IDs are stable; aliases can change
5. **Check recent activity**: `--from yesterday` or `--from "7 days ago"` for recent messages

## Troubleshooting

| Issue | Solution |
|-------|----------|
| No results | Check if indexer is running; verify database has messages |
| Connection error | Ensure MongoDB is accessible at configured URI |
| Slow queries | Use `--limit` to cap results; add time bounds |
| Missing messages | Check if room was indexed; verify bot had access |

## Notes

- Redacted messages omitted by default (use `--include-redacted` to show)
- Text matched as regex (case-insensitive by default)
- Database: `matrix_index` on MongoDB (localhost:27017)
- Tool: `/usr/local/bin/matrix-indexer-search`
- Repo: `patrick-slimelab/matrix-indexer.NET`