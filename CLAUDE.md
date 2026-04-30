# CLAUDE.md

## Project Overview

**hdx-scraper-ipc** scrapes Integrated Food Security Phase Classification (IPC) data from the IPC API and uploads it to the UN Humanitarian Data Exchange (HDX) platform. It creates two global datasets plus two datasets per country.

## Key Files

- `src/hdx/scraper/ipc/__main__.py` — orchestration entry point
- `src/hdx/scraper/ipc/ipc.py` — IPC data extraction and transformation (`IPC` class)
- `src/hdx/scraper/ipc/ipc_hapi.py` — HAPI-format dataset generation (`HAPIOutput` class)
- `src/hdx/scraper/ipc/config/project_configuration.yaml` — IPC API URL and settings
- `src/hdx/scraper/ipc/config/ch_countries.csv` — list of covered countries (ISO3)

## Running

```bash
python -m hdx.scraper.ipc
```

Requires these files in `$HOME`:
- `.hdx_configuration.yaml` — HDX API key, site, read_only flag
- `.useragents.yaml` — user agent config with key `hdx-scraper-ipc`
- `.extraparams.yaml` — IPC API key (or set `IPC_KEY` env var)

## Testing

```bash
pytest
# or
hatch test
```

Test fixtures live in `tests/fixtures/`. Expected output files are compared against generated output using `hdx.utilities.compare.assert_files_same`.

To update expected outputs after intentional changes, replace the fixture files with the newly generated ones.

## Code Style

- Formatted with `ruff` via pre-commit hooks (`hatch fmt --check` to verify)
- Python ≥ 3.13

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix. Unrelated changes obscure the intent of the fix and complicate review and blame.
