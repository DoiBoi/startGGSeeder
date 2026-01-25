# Player Rankings using glicko 2 system

Currently in heavy development.

To collaborators, I'm working on processing start.gg's return data, an example of how it would looks like is in items.json

Edit: It is now able to serialize and deserialize the data, what is now needed is to separate them by games

## Bulk tournament seeding (CLI)

This project includes a CLI that:
- Reads videogame IDs from the Supabase table `videogame_mapping`
- Queries start.gg for tournaments matching `(country/state/game IDs)` with pagination
- Processes each tournament by `slug`
- Stores the latest processed tournament `endAt` (unix timestamp) in Supabase table `last_updated`

### Usage

```powershell
py seed_tournaments.py --country CA --state BC --per-page 50
```

### Flags

- `--country` Country code filter (e.g. `CA`, `US`).
- `--state` State/province code filter (e.g. `BC`, `WA`).
- `--per-page` Page size for start.gg queries (default: `50`).
- `--before-date` Upper bound unix timestamp (e.g. `1769320800`).
- `--after-date` Lower bound unix timestamp override. If omitted, the tool uses the stored timestamp from `last_updated`.
- `--last-updated-key` The primary key string used in the `last_updated` table (default: `tournaments_endAt`).
- `--saved-games` / `--no-saved-games` If enabled (default), only processes events whose game IDs exist in `videogame_mapping`.
- `--dry-run` Lists matching tournaments and computes the max `endAt` without processing.

### Examples

Dry-run to see what would be processed:

```powershell
py seed_tournaments.py --country CA --state BC --dry-run
```

Backfill up to a specific timestamp:

```powershell
py seed_tournaments.py --country CA --state BC --before-date 1769320800
```


