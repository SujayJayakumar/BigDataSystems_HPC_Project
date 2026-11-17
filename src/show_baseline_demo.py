#!/usr/bin/env python3
"""Quickly display artifacts from a completed M100 Spark baseline run."""
import argparse
import csv
import json
from pathlib import Path

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', default='/workspace/outputs/median')
    parser.add_argument('--rows', type=int, default=8)
    args = parser.parse_args()
    root = Path(args.output)
    metrics_path = root / 'baseline_metrics.json'
    weights_path = root / 'top_feature_weights.csv'
    scores_path = root / 'baseline_scores'

    if not metrics_path.is_file() or not weights_path.is_file() or not scores_path.is_dir():
        raise SystemExit(f'Completed baseline artifacts not found under {root}. Run the baseline first.')

    metrics = json.loads(metrics_path.read_text(encoding='utf-8'))
    print('COMPLETED SPARK BASELINE')
    for key in ('input_files', 'sensor_feature_count', 'test_rows', 'test_pr_auc', 'total_seconds', 'model'):
        print(f'{key}: {metrics[key]}')
    print('\nTOP ABSOLUTE LOGISTIC COEFFICIENTS (first-pass feature explanation)')
    with weights_path.open(newline='', encoding='utf-8') as source:
        for row in list(csv.DictReader(source))[:5]:
            print(f"{row['feature']}: {float(row['coefficient']):+.6f}")

    spark = SparkSession.builder.appName('m100-baseline-output-demo').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    try:
        scores = spark.read.parquet(str(scores_path))
        print('\nSAVED HELD-OUT RISK SCORES')
        scores.select('node_id', 'timestamp', 'target_time', 'label', 'risk_score', 'decision') \
            .limit(max(1, args.rows)).show(max(1, args.rows), truncate=42)
        print(f'SCORES_PATH={scores_path}')
        print(f'METRICS_PATH={metrics_path}')
        print(f'WEIGHTS_PATH={weights_path}')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
