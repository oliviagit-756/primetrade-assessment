# MLOps Task 0 — Batch Signal Processor

A minimal MLOps-style batch job that reads OHLCV data, computes a rolling mean on the close price, generates a binary signal (`1` if close > rolling mean, else `0`), and outputs structured metrics with reproducible, deterministic runs.

## Project Structure
├── run.py              # Main pipeline script
├── config.yaml         # Configuration (seed, window, version)
├── data.csv            # Input OHLCV dataset (10,000 rows)
├── requirements.txt    # Python dependencies
├── Dockerfile          # Container definition
├── .dockerignore       # Files excluded from the container
├── README.md           # This file
├── metrics.json        # Sample output from a successful run
└── run.log             # Sample log from a successful run

## Requirements

- Python 3.11+
- Docker (for containerized runs)

## Local Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the pipeline
```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

### 3. Check outputs
- `metrics.json` — computed metrics
- `run.log` — detailed execution logs

## Docker Run

### Build the image
```bash
docker build -t mlops-task .
```

### Run the container
```bash
docker run --rm mlops-task
```

The container runs the full pipeline with default paths and prints the final metrics JSON to stdout. Exit code is `0` on success, `1` on failure.

## Configuration

`config.yaml`:
```yaml
seed: 42
window: 5
version: "v1"
```

- `seed` — random seed for reproducibility
- `window` — rolling window size for the mean calculation
- `version` — pipeline version (included in output)

## How It Works

1. **Load config** from YAML and validate required keys (`seed`, `window`, `version`)
2. **Set seed** via `numpy.random.seed()` for deterministic runs
3. **Load CSV** and validate it is non-empty and contains the `close` column. Column names are normalized to lowercase and stripped of whitespace on load for robustness.
4. **Compute rolling mean** on `close` with the configured window. The first `window - 1` rows produce `NaN`, leading to `signal = 0` for those rows (comparisons against `NaN` evaluate to `False`). This behavior is intentional and consistent.
5. **Generate signal**: `1` if `close > rolling_mean`, else `0`
6. **Compute metrics** (`rows_processed`, `signal_rate`, `latency_ms`) and write to JSON file + stdout

## Example Output

Success (`metrics.json`):
```json
{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.4989,     
  "latency_ms": 30,    
  "seed": 42,
  "status": "success"
}
```

Error (any validation/runtime failure):
```json
{
  "version": "v1",
  "status": "error",
  "error_message": "Missing required column: 'close'"
}
```

The `metrics.json` file is written in **both success and error cases**. Errors also produce a non-zero exit code so the failure is detectable by orchestration tools.

## Determinism

Runs with the same config and data produce identical `value`, `rows_processed`, and `seed` fields across every execution. Only `latency_ms` varies between runs, as timing is environment-dependent and not part of the logical output.

## Logging

All stages (config load, data load, rolling mean, signal generation, metrics summary, errors) are logged with timestamps and severity levels. Logs are written to both the configured log file and stdout for easy debugging in containerized environments.

## Error Handling

The following failure cases are handled cleanly with structured error output:

- Missing input file
- Empty CSV
- Invalid CSV format
- Missing `close` column
- Missing/invalid config file
- Missing required config keys

In all cases, the script writes an error `metrics.json`, logs the error, and exits with code `1`.