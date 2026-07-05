# Collector for IPC's Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-ipc/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-ipc/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-ipc/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-ipc?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This script connects to the [IPC API](https://www.ipcinfo.org/ipc-country-analysis/api/)
and extracts acute food security phase data, first creating 2 global and 2
per-country standard datasets in HDX, and then generating a separate HAPI food
security dataset. It makes reads to the IPC API
(one call to the `/analyses` endpoint, then one `/population` call and one
`/areas` geojson call per country with active data), a read to InterAction,
and HDX read/writes for the per-country and global datasets.
It creates temporary CSV files (6 per country
covering national, admin-1, and area granularities in both long and wide
formats, a few KB each) which are uploaded to HDX. The IPC API JSON responses are pivoted
from wide phase columns (phases 1–5, IPC Plus) into long-format rows;
population-in-phase values are disaggregated by projection period (current,
first, and second); locations are matched to admin P-codes before being written to the standard IPC
datasets; the HAPI food security dataset is then generated from the same
processed data.

## Data Pipeline

### API reads

- **IPC /analyses** (1 read): fetches the list of active analyses and country
  metadata.
- **Per-country IPC data** (~2 reads per country): one
  `/population` call and one `/areas` geojson call per country with active data.
- **InterAction** (1 read): supplementary organisation data.

### API writes

- **Per-country standard datasets** (~2 writes per country): each country has one
  long-format and one wide-format dataset covering national, admin-1, and area
  granularities (6 CSV files per country).
- **Global standard datasets** (2 writes): global long-format and wide-format IPC
  datasets.
- **HAPI food security dataset** (1 write): derived from the same processed data.

### Temporary files

- CSV files (6 per country: national, admin-1, and area granularities ×
  long and wide formats), a few KB each.

### Uploaded files

- 6 CSV files per country (national/admin-1/area × long/wide format).
- 2 global standard IPC datasets (long and wide format).
- HAPI food security dataset.

### Transformations

1. **Phase pivot**: IPC API JSON responses are converted from wide phase columns
   (phases 1–5 and IPC Plus) into long-format rows, one row per phase.
2. **Projection disaggregation**: population-in-phase values are split by
   projection period (current, first projection, and second projection).
3. **P-code matching**: location strings are matched to admin P-codes using the
   COD admin boundary registry.

## Development

### Environment

Development is currently done using Python 3.13. The environment can be created with:

```shell
    uv sync
```

This creates a .venv folder with the versions specified in the project's uv.lock file.

### Installing and running

For the script to run, you will need to have a file called
.hdx_configuration.yaml in your home directory containing your HDX key, e.g.:

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home
 directory as specified in the parameter *user_agent_config_yaml* passed to
 facade in run.py. The collector reads the key
 **hdx-scraper-ipc** as specified in the parameter
 *user_agent_lookup*.

 Alternatively, you can set up environment variables: `USER_AGENT`, `HDX_KEY`,
`HDX_SITE`, `EXTRA_PARAMS`, `TEMP_DIR`, and `LOG_FILE_ONLY`.

To run, execute:

```shell
    uv run python -m hdx.scraper.ipc
```

### Pre-commit

pre-commit will be installed when syncing uv. It is run every time you make a git
commit if you call it like this:

```shell
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

## Packages

[uv](https://github.com/astral-sh/uv) is used for package management.  If
you've introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `project.dependencies` section of `pyproject.toml` with
any known version constraints.

To add packages required only for testing, add them to the
`[dependency-groups]`.

Any changes to the dependencies will be automatically reflected in
`uv.lock` with `pre-commit`, but you can re-generate the files without committing by
executing:

```shell
    uv lock --upgrade
```

## Project

[uv](https://github.com/astral-sh/uv) is used for project management. The project can be
built using:

```shell
    uv build
```

Linting and syntax checking can be run with:

```shell
    uv run ruff check
```

To run the tests and view coverage, execute:

```shell
    uv run pytest
```
