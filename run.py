import argparse
import json
import logging
import sys
import time

import numpy as np
import pandas as pd
import yaml


def setup_logging(log_file_path):
    """Configure logging to write to both file and terminal."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler()
        ]
    )


def load_config(config_path):
    """Load and validate the YAML config file."""
    logging.info(f"Loading config from {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    if config is None:
        raise ValueError("Config file is empty or invalid YAML")

    required_keys = ["seed", "window", "version"]
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required config key: {key}")

    logging.info(
        f"Config loaded and validated: "
        f"seed={config['seed']}, window={config['window']}, version={config['version']}"
    )
    return config


def load_data(input_path):
    """Load and validate the input CSV."""
    logging.info(f"Loading data from {input_path}")

    df = pd.read_csv(input_path)

    # Normalize column names (lowercase + strip whitespace)
    df.columns = df.columns.str.lower().str.strip()

    if df.empty:
        raise ValueError("Input CSV is empty")

    if "close" not in df.columns:
        raise ValueError("Missing required column: 'close'")

    logging.info(f"Loaded {len(df)} rows from {input_path}")
    return df


def process_data(df, window):
    """Compute rolling mean on 'close' and generate binary signal."""
    logging.info(f"Computing rolling mean with window={window}")
    df["rolling_mean"] = df["close"].rolling(window=window).mean()

    logging.info("Generating binary signal (close > rolling_mean)")
    df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)

    logging.info("Signal generation complete")
    return df


def write_metrics(output_path, metrics):
    """Write metrics dictionary to JSON file."""
    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2)
    logging.info(f"Metrics written to {output_path}")


def main():
    # Start timer at the very beginning
    start_time = time.time()

    # Parse CLI arguments
    parser = argparse.ArgumentParser(description="MLOps Task 0 - Batch signal processor")
    parser.add_argument("--input", required=True, help="Path to input CSV")
    parser.add_argument("--config", required=True, help="Path to config YAML")
    parser.add_argument("--output", required=True, help="Path to output metrics JSON")
    parser.add_argument("--log-file", required=True, help="Path to log file")
    args = parser.parse_args()

    # Set up logging
    setup_logging(args.log_file)
    logging.info("Job started")

    # Default version used if config fails to load
    version = "v1"

    try:
        # Load and validate config
        config = load_config(args.config)
        version = config["version"]

        # Set seed for reproducibility
        np.random.seed(config["seed"])
        logging.info(f"Random seed set to {config['seed']}")

        # Load and process data
        df = load_data(args.input)
        df = process_data(df, config["window"])

        # Compute latency
        latency_ms = int((time.time() - start_time) * 1000)

        # Build success metrics
        metrics = {
            "version": config["version"],
            "rows_processed": len(df),
            "metric": "signal_rate",
            "value": round(float(df["signal"].mean()), 4),
            "latency_ms": latency_ms,
            "seed": config["seed"],
            "status": "success"
        }

        logging.info(f"Metrics: {metrics}")
        write_metrics(args.output, metrics)
        print(json.dumps(metrics, indent=2))
        logging.info("Job ended successfully")

    except Exception as e:
        logging.error(f"Job failed: {e}")

        error_metrics = {
            "version": version,
            "status": "error",
            "error_message": str(e)
        }

        write_metrics(args.output, error_metrics)
        print(json.dumps(error_metrics, indent=2))
        logging.info("Job ended with status: error")
        sys.exit(1)


if __name__ == "__main__":
    main()