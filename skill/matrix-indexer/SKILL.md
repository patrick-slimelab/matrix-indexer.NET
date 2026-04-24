---
name: Matrix Indexer Search
description: Search indexed Matrix messages using the `matrix-indexer-search` CLI tool. Supports regex, date ranges, room/sender filtering, index coverage stats, and oldest/newest checks.
---

## Usage

```bash
matrix-indexer-search "<regex>" [--limit N] [--room ROOM_ID] [--sender SENDER] [--from TIME] [--to TIME] [--oldest|--newest]
matrix-indexer-search --stats
```

## First checks

When asked how far back the index goes, whether backfill is done, or how complete the DB is, **do not infer from a normal search**. Run:

```bash
matrix-indexer-search --stats
```

Normal searches default to **newest-first**. To find the earliest indexed message, use:

```bash
matrix-indexer-search ".*" --oldest --limit 1
```

## Options

| Option | Description |
|--------|-------------|
| `--stats` | Print DB counts, oldest/newest message, messages by year, and backfill queue counts |
| `--limit N` | Max rows to return (default: 20) |
| `--room ROOM_ID` | Restrict to a Matrix room_id (e.g., `!abc123:example.com`) |
| `--sender SENDER` | Restrict to sender mxid (e.g., `@user:example.com`) |
| `--from TIME` | Lower time bound (ISO-8601 or unix ms) |
| `--to TIME` | Upper time bound (ISO-8601 or unix ms) |
| `--oldest` | Sort oldest-first; use this for earliest-message checks |
| `--newest` | Sort newest-first (default) |
| `--case-sensitive` | Case-sensitive regex match |
| `--include-redacted` | Include redacted messages |

## Examples

```bash
# Coverage / backfill status
matrix-indexer-search --stats

# Earliest indexed searchable message
matrix-indexer-search ".*" --oldest --limit 1

# Latest indexed searchable message (default sort)
matrix-indexer-search ".*" --limit 1

# Basic keyword search
matrix-indexer-search "keyword" --limit 50

# Search specific room
matrix-indexer-search "bot" --room "!roomId:example.com"

# Search date range, oldest first
matrix-indexer-search " deploy " --from 2026-03-01 --to 2026-04-01 --oldest

# Search from specific sender
matrix-indexer-search "announcement" --sender "@admin:example.com"

# Regex search (find URLs)
matrix-indexer-search "https?://[^ ]+" --limit 30
```

## Search Tips

1. **Use `--stats` for coverage/backfill questions.** It reports oldest/newest and remaining backfill rooms directly.
2. **Remember default sort is newest-first.** Add `--oldest` when checking historical floor.
3. **Start broad, then refine** with room/sender/time filters.
4. **Regex is powerful**: Search for patterns like URLs, emails, version numbers.
5. **Room IDs over aliases**: Room IDs are stable; aliases can change.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Oldest result looks too recent | You probably used default newest-first sort; rerun with `--oldest` or `--stats` |
| No results | Check if indexer is running; run `matrix-indexer-search --stats` |
| Connection error | Ensure MongoDB is accessible at configured URI |
| Slow queries | Use `--limit` and add time bounds |
| Missing messages | Check if room was indexed and bot had access; use `--stats` for backfill queue status |

## Notes

- Redacted messages omitted by default (use `--include-redacted` to show)
- Text matched as regex (case-insensitive by default)
- Database: `matrix_index` on MongoDB (localhost:27017)
- Tool: `/usr/local/bin/matrix-indexer-search`
- Repo: `patrick-slimelab/matrix-indexer.NET`
