#!/usr/bin/env python3
"""Run the first chronological M100 node-risk prediction baseline."""
import argparse
import json
import time
from datetime import timedelta
from pathlib import Path

from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, FloatType, IntegerType, LongType, ShortType, ByteType
from pyspark.sql.window import Window

NUMERIC_TYPES = (DoubleType, FloatType, IntegerType, LongType, ShortType, ByteType)


def colq(name):
    return F.col('`' + name.replace('`', '``') + '`')


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--data', default='/workspace/data/raw/parquet')
    ap.add_argument('--output', default='/workspace/outputs')
    ap.add_argument('--max-files', type=int, default=0, help='Use only the first N node files for a smoke run')
    ap.add_argument('--strategy', choices=('median', 'drop'), default='median')
    ap.add_argument('--threshold', type=float, default=0.5)
    ap.add_argument('--max-iter', type=int, default=10)
    args = ap.parse_args()
    started = time.time()
    output = Path(args.output) / args.strategy
    output.mkdir(parents=True, exist_ok=True)
    spark = SparkSession.builder.appName('m100-next-interval-risk-baseline').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        root = Path(args.data)
        files = sorted(root.rglob('*.parquet')) if root.exists() else []
        if args.max_files > 0:
            files = files[:args.max_files]
        if not files:
            raise SystemExit(f'No Parquet node files found under {root}; download and extract the archives first.')
        print(f'INPUT_FILES={len(files)}')
        raw = spark.read.option('mergeSchema', 'true').parquet(*[str(p) for p in files])
        if 'timestamp' not in raw.columns or 'value' not in raw.columns:
            raise SystemExit('Expected M100 timestamp and value columns were not found.')
        sensor_cols = [f.name for f in raw.schema.fields
                       if isinstance(f.dataType, NUMERIC_TYPES) and f.name not in ('value', '__index_level_0__')]
        if not sensor_cols:
            raise SystemExit('No numeric sensor columns found.')
        print(f'SENSOR_FEATURES={len(sensor_cols)}')

        # Each file represents a node. Value uses Nagios codes: 0=OK, 1=WARNING,
        # 2=CRITICAL, 3=UNKNOWN; null and UNKNOWN target intervals are excluded.
        rows = (raw.withColumn('_path', F.input_file_name())
                .withColumn('node_id', F.regexp_extract('_path', r'([^/\\]+)\.parquet$', 1))
                .withColumn('timestamp', F.col('timestamp').cast('timestamp'))
                .filter(F.col('timestamp').isNotNull()))
        by_node_time = Window.partitionBy('node_id').orderBy('timestamp')
        rows = (rows.withColumn('target_time', F.lead('timestamp').over(by_node_time))
                .withColumn('_target_status', F.lead('value').over(by_node_time))
                .filter(F.col('_target_status').isin(0, 1, 2))
                .withColumn('label', F.when(F.col('_target_status') == 0, 0.0).otherwise(1.0))
                .drop('value', '_target_status', '__index_level_0__', '_path'))
        normalized = [F.nanvl(colq(c).cast('double'), F.lit(None).cast('double')).alias(c) if c in sensor_cols else colq(c) for c in rows.columns]
        rows = rows.select(*normalized)

        bounds = rows.agg(F.min('target_time').alias('lo'), F.max('target_time').alias('hi')).first()
        lo, hi = bounds['lo'], bounds['hi']
        if lo is None or hi is None or lo >= hi:
            raise SystemExit('Not enough labeled next-interval data for chronological train/validation/test splits.')
        seconds = (hi - lo).total_seconds()
        train_end_dt = lo + timedelta(seconds=seconds * 0.70)
        valid_end_dt = lo + timedelta(seconds=seconds * 0.85)
        train_end, valid_end = F.lit(train_end_dt), F.lit(valid_end_dt)
        train = rows.filter(F.col('target_time') < train_end)
        valid = rows.filter((F.col('target_time') >= train_end) & (F.col('target_time') < valid_end))
        test = rows.filter(F.col('target_time') >= valid_end)
        print(f'TARGET_TIME_RANGE={lo} .. {hi}; TRAIN_END={train_end_dt}; VALID_END={valid_end_dt}')

        train_counts = {float(r['label']): int(r['count']) for r in train.groupBy('label').count().collect()}
        if train_counts.get(0.0, 0) == 0 or train_counts.get(1.0, 0) == 0:
            raise SystemExit(f'Train split needs normal and critical labels; got {train_counts}.')
        nonnull = train.agg(*[F.count(colq(c)).alias(f'c{i}') for i, c in enumerate(sensor_cols)]).first().asDict()
        usable = [c for i, c in enumerate(sensor_cols) if nonnull[f'c{i}'] > 0]
        all_missing = [c for c in sensor_cols if c not in usable]
        if not usable:
            raise SystemExit('All sensor columns are missing in the training period.')

        if args.strategy == 'drop':
            train, valid, test = (x.dropna(subset=usable) for x in (train, valid, test))
            train_counts = {float(r['label']): int(r['count']) for r in train.groupBy('label').count().collect()}
            if train_counts.get(0.0, 0) == 0 or train_counts.get(1.0, 0) == 0:
                raise SystemExit(f'After dropping missing rows, train split has only one label class: {train_counts}.')
            prep = [VectorAssembler(inputCols=usable, outputCol='features', handleInvalid='skip')]
            model_feature_names = usable
        else:
            indicators = [f'__missing_{i}' for i in range(len(usable))]
            train, valid, test = tuple(
                frame.select('*', *[F.col(c).isNull().cast('double').alias(flag) for c, flag in zip(usable, indicators)])
                for frame in (train, valid, test))
            imputed = [f'__imputed_{i}' for i in range(len(usable))]
            prep = [Imputer(strategy='median', relativeError=0.01, inputCols=usable, outputCols=imputed),
                    VectorAssembler(inputCols=imputed + indicators, outputCol='features', handleInvalid='skip')]
            model_feature_names = usable + [f'missing:{c}' for c in usable]

        total_train = sum(train_counts.values())
        class_weights = {label: total_train / (2 * count) for label, count in train_counts.items()}
        def add_weight(frame):
            return frame.withColumn('class_weight', F.when(F.col('label') == 1.0, F.lit(class_weights[1.0])).otherwise(F.lit(class_weights[0.0])))
        train, valid, test = (add_weight(frame) for frame in (train, valid, test))

        lr = LogisticRegression(labelCol='label', featuresCol='features', weightCol='class_weight',
                                maxIter=args.max_iter, regParam=0.01, elasticNetParam=0.0)
        pipeline = Pipeline(stages=prep + [lr])
        fit_started = time.time()
        fitted = pipeline.fit(train)
        fit_seconds = time.time() - fit_started
        model_path = output / 'model'
        fitted.write().overwrite().save(str(model_path))
        anomaly_probability = vector_to_array('probability')[1]
        scored = (fitted.transform(test)
                  .withColumn('risk_score', anomaly_probability)
                  .withColumn('decision', F.when(F.col('risk_score') >= args.threshold, 'alert').otherwise('monitor'))).persist(StorageLevel.DISK_ONLY)
        pr_auc = BinaryClassificationEvaluator(labelCol='label', rawPredictionCol='rawPrediction',
                                               metricName='areaUnderPR').evaluate(scored)
        score_path = output / 'baseline_scores'
        (scored.select('node_id', 'timestamp', 'target_time', 'label', 'risk_score', 'prediction', 'decision')
         .withColumn('year', F.year('target_time')).withColumn('month', F.month('target_time'))
         .write.mode('overwrite').partitionBy('year', 'month').parquet(str(score_path)))

        lr_model = fitted.stages[-1]
        top = sorted(zip(model_feature_names, map(float, lr_model.coefficients)), key=lambda item: abs(item[1]), reverse=True)[:30]
        with (output / 'top_feature_weights.csv').open('w', encoding='utf-8', newline='') as f:
            f.write('feature,coefficient\n')
            for name, value in top:
                f.write('"' + name.replace('"', '""') + f'",{value}\n')
        score_count = scored.count()
        scored.unpersist()
        metrics = {
            'input_files': len(files), 'sensor_feature_count': len(sensor_cols),
            'usable_feature_count': len(usable), 'all_missing_in_train': all_missing,
            'nagios_label_mapping': 'next value 0 (OK) -> normal; 1 (WARNING) or 2 (CRITICAL) -> anomaly; 3 (UNKNOWN) and null excluded',
            'train_label_counts': {str(k): v for k, v in train_counts.items()},
            'target_time_min': lo.isoformat(), 'target_time_max': hi.isoformat(),
            'train_end': train_end_dt.isoformat(), 'validation_end': valid_end_dt.isoformat(),
            'cleaning_strategy': args.strategy,
            'model': 'class-weighted Spark ML LogisticRegression; telemetry at t predicts next recorded interval',
            'test_pr_auc': float(pr_auc), 'baseline_threshold': args.threshold, 'max_iterations': args.max_iter,
            'test_rows': score_count, 'fit_seconds': fit_seconds, 'total_seconds': time.time() - started,
            'interpretation_note': 'Nagios status is an operational label, not a confirmed hardware-fault cause.'
        }
        (output / 'baseline_metrics.json').write_text(json.dumps(metrics, indent=2), encoding='utf-8')
        print(json.dumps(metrics, indent=2))
        print(f'SAVED_SCORES={score_path}')
        print(f'SAVED_METRICS={output / "baseline_metrics.json"}')
        print(f'SAVED_COEFFICIENTS={output / "top_feature_weights.csv"}')
        print('SPARK_UI=http://localhost:4040 (available while this run is active)')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()










