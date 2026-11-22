from pyspark.sql.functions import avg, count, round
import argparse

def main(args):

    catalog = args.catalog
    silver_schema = args.silver_schema
    gold_schema = args.gold_schema

    df = spark.read.table(f"{catalog}.{silver_schema}.jc_citibike")

    df = df.\
        groupBy("trip_start_date", "start_station_name").\
        agg(
        round(avg("trip_duration_mins"),2).alias("avg_trip_duration_mins"),
        count("ride_id").alias("total_trips")
        )

    df.write.\
        mode("overwrite").\
        option("overwriteSchema", "true").\
        saveAsTable(f"{catalog}.{gold_schema}.daily_station_performance")

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", type=str, required=True)
    parser.add_argument("--silver_schema", type=str, required=True)
    parser.add_argument("--gold_schema", type=str, required=True)

    args = parser.parse_args()

    main(args)