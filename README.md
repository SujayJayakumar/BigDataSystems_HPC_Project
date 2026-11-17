# M100 HPC Node Health Risk Application

Team: Sujay Jayakumar and Yi Hung Lim | EN.601 Big Data Systems, Section 02

This is the implementation for the approved Option B project. It uses the public M100 supercomputer node telemetry dataset. The 24.8 GB source is split into rack archives and has one Parquet file per node (980+ nodes), 15-minute telemetry summaries, and Nagios operational status labels. Source data is kept out of Git. The dataset is CC BY 4.0; see the citation below.

## What the first baseline does

The starter pipeline scans the Parquet files with Spark, treats each node's file as its time series, and uses the telemetry at time *t* to predict the next recorded Nagios status. In the observed sample, the `value` field contains 0, 2, 3, and null. The baseline maps Nagios 0 (OK) to normal and 1 (WARNING) or 2 (CRITICAL) to anomaly; it excludes 3 (UNKNOWN) and missing target labels. This is an operational prediction target, not proof of a hardware fault.

The baseline uses a chronological 70/15/15 train/validation/test split, median imputation plus missingness flags by default, and a class-weighted Spark ML logistic regression. It reports test PR-AUC, writes scored test rows as Parquet partitioned by target year/month, and saves the model's largest logistic coefficients as a first-pass explanation. The `drop` option runs the comparison that drops rows missing any selected sensor field. Threshold calibration, one-hour rolling features, the stronger tree model, node/time stability analysis, and Spark tuning are planned next steps; they are not claimed as completed by this initial baseline.

## Setup (Windows or macOS)

Requirements: Docker Desktop with Compose, Python 3.10+ for the download/extraction helpers, and enough free disk for the source archives and extracted data. The official Apache Spark 3.5.3 Python image keeps Spark and Java consistent across Windows and macOS (including Apple Silicon). Do not commit `data/` or generated outputs.

1. Build the portable Spark image once: `docker compose build spark` (uses the pinned NumPy and Pandas dependencies required by Spark ML).
2. Download and verify the Zenodo archives (safe to rerun; verified archives are skipped):
   - Windows PowerShell: `python scripts/download_m100.py`
   - macOS/Linux: `python3 scripts/download_m100.py`
3. Extract all rack archives:
   - Windows: `python scripts/extract_dataset.py`
   - macOS/Linux: `python3 scripts/extract_dataset.py`
4. Run the schema / label summary:
   - `docker compose run --rm spark /opt/spark/bin/spark-submit --master 'local[*]' /workspace/src/inspect_dataset.py`
5. Run the full median-imputation baseline and keep the Spark UI reachable at `http://localhost:4040` while it runs:
   - `docker compose run --rm --service-ports spark /opt/spark/bin/spark-submit --driver-memory 8g --master 'local[*]' /workspace/src/run_baseline.py`
6. Run the row-drop comparison:
   - `docker compose run --rm --service-ports spark /opt/spark/bin/spark-submit --driver-memory 8g --master 'local[*]' /workspace/src/run_baseline.py --strategy drop`

For a repeatable smoke run on the already-extracted example rack, use `--data /workspace/data/raw/parquet/rack_37 --max-files 15`. This is only a development check, not a project result. The default complete run reads all extracted nodes.

## Outputs and evidence

- `outputs/median/model/` or `outputs/drop/model/`: fitted Spark ML pipeline.
- `outputs/median/baseline_metrics.json` or `outputs/drop/baseline_metrics.json`: label handling, split dates, timing, and test PR-AUC.
- `outputs/median/baseline_scores/` or `outputs/drop/baseline_scores/`: test risk scores and alert/monitor decisions, partitioned by target year and month.
- `outputs/median/top_feature_weights.csv` or `outputs/drop/top_feature_weights.csv`: largest absolute logistic coefficients, a basic model explanation.
- `evidence/`: keep Spark UI / plan captures and comparison notes here.
- `data/processed/`: reserved for cleaned analysis-ready Parquet tables in later pipeline steps.

## Project questions and next steps

The approved questions are whether cleaning choices change risk predictions, whether prior one-hour rolling features improve next-interval prediction, and whether performance / feature explanations remain stable across held-out nodes and later months. Phase 2 establishes the baseline and captures the slowest stage, physical plan, run time, and output. Phase 3 can use those measurements to choose and evaluate one targeted Spark optimization. No tuning claim should be made before collecting the baseline evidence.

## Data citation

Borghesi, A. et al. (2023). *M100 dataset: time-aggregated data for anomaly detection*. Zenodo. https://doi.org/10.5281/zenodo.7541722. CC BY 4.0.

## Quick presentation demo

To show the saved baseline result without retraining, run:

```bash
docker compose run --rm spark /opt/spark/bin/spark-submit --master 'local[*]' /workspace/src/show_baseline_demo.py
```

This reads the saved sample metrics, prints five largest feature weights, and displays a few saved held-out risk scores. It does not fit the model again. The full baseline command above takes much longer.
