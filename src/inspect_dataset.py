#!/usr/bin/env python3
"""Summarize the M100 Parquet schema and identify the anomaly label."""
import argparse
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

LABEL_HINTS = ('nagios', 'anomaly', 'label', 'alarm', 'alert', 'flag', 'status')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', default='/workspace/data/raw/parquet')
    args = parser.parse_args()
    root = Path(args.data)
    spark = SparkSession.builder.appName('m100-inspect').master('local[*]').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    try:
        df = spark.read.option('recursiveFileLookup', 'true').option('mergeSchema', 'true').parquet(args.data)
        fields = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        print('PARQUET_FILES=', len(list(root.rglob('*.parquet'))))
        print('COLUMN_COUNT=', len(fields))
        print('TIME_FIELDS=', {k:v for k,v in fields.items() if any(x in k.lower() for x in ('timestamp','time'))})
        print('IDENTIFIER_FIELDS=', {k:v for k,v in fields.items() if any(x in k.lower() for x in ('node','host','rack'))})
        print('LABEL_NAME_CANDIDATES=', {k:v for k,v in fields.items() if any(x in k.lower() for x in LABEL_HINTS)})
        print('VALUE_FIELD_TYPE=', fields.get('value'))
        cols = [c for c in ('timestamp','value','__index_level_0__') if c in df.columns]
        print('ROW_COUNT=', df.count())
        if cols:
            print('TIME_LABEL_SAMPLE:')
            df.select(*cols).show(8, truncate=60)
        if 'value' in df.columns:
            print('VALUE_COUNTS:')
            df.groupBy('value').count().orderBy(F.desc('count')).show(20, truncate=60)
        print('SAMPLE_NUMERIC_FIELDS=', [(f.name, f.dataType.simpleString()) for f in df.schema.fields if f.dataType.simpleString() in ('double','float','int','bigint','smallint','tinyint')][:30])
    finally:
        spark.stop()

if __name__ == '__main__': main()

