import argparse

#import sys
#import os
#current_dir = os.getcwd()
#project_root = os.path.abspath(os.path.join(current_dir, "..", "..", ".." ))
#sys.path.append(project_root)

#from src.citibike.citibike_utils import get_trip_duration_mins
#from src.utils.datetime_utils import timestamp_to_date_col

from citibike.citibike_utils import get_trip_duration_mins
from utils.datetime_utils import timestamp_to_date_col
from pyspark.sql.functions import create_map, lit

def main(args):

    pipeline_id = args.pipeline_id
    run_id = args.run_id
    task_id = args.task_id
    processed_timestamp = args.processed_timestamp
    catalog = args.catalog
    bronze_schema = args.bronze_schema
    silver_schema = args.silver_schema

    df = spark.read.table(f"{catalog}.{bronze_schema}.jc_citibike")

    df = get_trip_duration_mins(spark, df, "started_at", "ended_at", "trip_duration_mins")

    df = timestamp_to_date_col(spark, df, "started_at", "trip_start_date")

    df = df.withColumn("metadata",
                       create_map(
                           lit("pipeline_id"), lit(pipeline_id),
                           lit("run_id"), lit(run_id),
                           lit("task_id"), lit(task_id),
                           lit("processed_date"), lit(processed_timestamp)
                       ))

    df = df.select(
        "ride_id",
        "trip_start_date",
        "started_at",
        "ended_at",
        "start_station_name",
        "end_station_name",
        "trip_duration_mins",
        "metadata"
    )

    df.write. \
        mode("overwrite"). \
        option("overwriteSchema", "true"). \
        saveAsTable(f"{catalog}.{silver_schema}.jc_citibike")

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline_id", type=str, required=True)
    parser.add_argument("--run_id", type=str, required=True)
    parser.add_argument("--task_id", type=str, required=True)
    parser.add_argument("--processed_timestamp", type=str, required=True)
    parser.add_argument("--catalog", type=str, required=True)
    parser.add_argument("--bronze_schema", type=str, required=True)
    parser.add_argument("--silver_schema", type=str, required=True)

    args = parser.parse_args()

    main(args)