#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf


@udf('boolean')
def is_purchase(event_as_json):
    event = json.loads(event_as_json)
#     if event['event_type'] == 'purchase_sword':
#         return True
    if 'purchase' in event['event_type']:
        return True
    return False


@udf('boolean')
def is_guild(event_as_json):
    event = json.loads(event_as_json)
#     if event['event_type'] == 'purchase_sword':
#         return True
    if 'guild' in event['event_type']:
        return True
    return False


def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .enableHiveSupport() \
        .getOrCreate()

    raw_events = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    # create HDFS tables for purchase events
    purchase_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_purchase('raw'))

    extracted_purchase_events = purchase_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_purchase_events.printSchema()
    extracted_purchase_events.show()

    extracted_purchase_events.registerTempTable("extracted_purchase_events")

    # this sends the table to hive
    spark.sql("drop table if exists purchases")
    spark.sql("""
        create external table if not exists purchases
        stored as parquet
        location '/tmp/purchases'
        as
        select * from extracted_purchase_events
    """)
    
    
    # Create HDFS tables for guild stuff
    guild_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_guild('raw'))

    extracted_guild_events = guild_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_guild_events.printSchema()
    extracted_guild_events.show()

    extracted_guild_events.registerTempTable("extracted_guild_events")

    # this sends the table to hive
    spark.sql("drop table if exists guilds")
    spark.sql("""
        create external table if not exists guilds
        stored as parquet
        location '/tmp/guilds'
        as
        select * from extracted_guild_events
    """)


if __name__ == "__main__":
    main()
