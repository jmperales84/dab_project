from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql.functions import create_map, lit
import argparse

def main(args):

    pipeline_id = args.pipeline_id
    run_id = args.run_id
    task_id = args.task_id
    processed_timestamp = args.processed_timestamp
    catalog = args.catalog
    bronze_schema = args.bronze_schema

    schema = StructType([
        StructField("ride_id", StringType(), True),
        StructField("rideable_type", StringType(), True),
        StructField("started_at", TimestampType(), True),
        StructField("ended_at", TimestampType(), True),
        StructField("start_station_name", StringType(), True),
        StructField("start_station_id", StringType(), True),
        StructField("end_station_name", StringType(), True),
        StructField("end_station_id", StringType(), True),
        StructField("start_lat", DecimalType(), True),
        StructField("start_lng", DecimalType(), True),
        StructField("end_lat", DecimalType(), True),
        StructField("end_lng", DecimalType(), True),
        StructField("member_casual", StringType(), True),
    ])

    df = spark.read.csv(f"/Volumes/{catalog}/00_landing/source_citibike_data/JC-202503-citibike-tripdata.csv",
                        schema=schema,
                        header=True
    )

    df = df.withColumn("metadata",
                  create_map(
                      lit("pipeline_id"), lit(pipeline_id),
                      lit("run_id"), lit(run_id),
                      lit("task_id"), lit(task_id),
                      lit("processed_date"), lit(processed_timestamp)
                  )
    )

    df.write.\
        mode("overwrite").\
        option("overwriteSchema", "true").\
        saveAsTable(f"{catalog}.{bronze_schema}.jc_citibike")

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline_id", type=str, required=True)
    parser.add_argument("--run_id", type=str, required=True)
    parser.add_argument("--task_id", type=str, required=True)
    parser.add_argument("--processed_timestamp", type=str, required=True)
    parser.add_argument("--catalog", type=str, required=True)
    parser.add_argument("--bronze_schema", type=str, required=True)

    args = parser.parse_args()

    main(args)