#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType

# define our own schema so no more surprises
def purchase_sword_event_schema():
    """
    root
     |-- Accept: string (nullable = true)
     |-- Host: string (nullable = true)
     |-- User-Agent: string (nullable = true)
     |-- event_type: string (nullable = true)
     |-- type: string (nullable = true)
     |-- cost: long (nullable = true)
    """
    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("type", StringType(), True),
        StructField("cost", LongType(), True),
    ])

def purchase_event_schema():
    """
    root
     |-- Accept: string (nullable = true)
     |-- Host: string (nullable = true)
     |-- User-Agent: string (nullable = true)
     |-- event_type: string (nullable = true)
     |-- type: string (nullable = true)
     |-- cost: long (nullable = true)
    """
    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("type", StringType(), True),
        StructField("cost", LongType(), True),
    ])


def guild_event_schema():
    """
    root
     |-- Accept: string (nullable = true)
     |-- Host: string (nullable = true)
     |-- User-Agent: string (nullable = true)
     |-- event_type: string (nullable = true)
     |-- guild_member_cnt: long (nullable = true)
     |-- guild_name: string (nullable = true)
     |-- tot_guild_member_cnt: long (nullable = true)
    """
    return StructType([
        StructField("Accept", StringType(), True),
        StructField("Host", StringType(), True),
        StructField("User-Agent", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("guild_member_cnt", LongType(), True),
        StructField("guild_name", StringType(), True),
        StructField("tot_guild_member_cnt", LongType(), True),
    ])


@udf('boolean')
def is_sword_purchase(event_as_json):
    """udf for filtering events
    """
    event = json.loads(event_as_json)
    if event['event_type'] == 'purchase_sword':
        return True
    return False


@udf('boolean')
def is_purchase(event_as_json):
    event = json.loads(event_as_json)
    if 'purchase' in event['event_type']:
        return True
    return False


@udf('boolean')
def is_guild(event_as_json):
    event = json.loads(event_as_json)
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
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .load()

    sword_purchases = raw_events \
        .filter(is_sword_purchase(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          purchase_sword_event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')

    
    spark.sql("drop table if exists sword_purchases") # will get errors when rerunning w/o this
    # here we hand over the schema explicitly, last time we registered a temp table 
    sql_string = """
        create external table if not exists sword_purchases (
            raw_event string,
            timestamp string,
            Accept string,
            Host string,
            `User-Agent` string,
            event_type string,
            type string,
            cost long
            )
            stored as parquet
            location '/tmp/sword_purchases'
            tblproperties ("parquet.compress"="SNAPPY")
            """
    spark.sql(sql_string)

    sink = sword_purchases \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_sword_purchases") \
        .option("path", "/tmp/sword_purchases") \
        .trigger(processingTime="10 seconds") \
        .start()

    ## Extract the purchase events
    extracted_purchase_events = raw_events \
        .filter(is_purchase(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          purchase_event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')
                
    extracted_purchase_events.printSchema()
    # extracted_purchase_events.show()
    # extracted_purchase_events.registerTempTable("extracted_purchase_events")
    
    spark.sql("drop table if exists purchases") # will get errors when rerunning w/o this
    # here we hand over the schema explicitly, in batch we previously registered a temp table 
    sql_string2 = """
        create external table if not exists purchases (
            raw_event string,
            timestamp string,
            Accept string,
            Host string,
            `User-Agent` string,
            event_type string,
            type string,
            cost long
            )
            stored as parquet
            location '/tmp/purchases'
            tblproperties ("parquet.compress"="SNAPPY")
            """
    spark.sql(sql_string2)

    sink = extracted_purchase_events \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_purchase_events") \
        .option("path", "/tmp/purchases") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    ## Extract the guild events
    extracted_guild_events = raw_events \
        .filter(is_guild(raw_events.value.cast('string'))) \
        .select(raw_events.value.cast('string').alias('raw_event'),
                raw_events.timestamp.cast('string'),
                from_json(raw_events.value.cast('string'),
                          guild_event_schema()).alias('json')) \
        .select('raw_event', 'timestamp', 'json.*')
                
    extracted_guild_events.printSchema()
    # extracted_guild_events.show()
    # extracted_guild_events.registerTempTable("extracted_guild_events")
    
    spark.sql("drop table if exists guilds") # will get errors when rerunning w/o this
    # here we hand over the schema explicitly, last time we registered a temp table 
    sql_string3 = """
        create external table if not exists guilds (
            raw_event string,
            timestamp string,
            Accept string,
            Host string,
            `User-Agent` string,
            event_type string,
            guild_member_cnt long,
            guild_name string,
            tot_guild_member_cnt long
            )
            stored as parquet
            location '/tmp/guilds'
            tblproperties ("parquet.compress"="SNAPPY")
            """
    spark.sql(sql_string3)

    sink = extracted_guild_events \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints_for_guild_events") \
        .option("path", "/tmp/guilds") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    spark.streams.awaitAnyTermination()  # https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html


if __name__ == "__main__":
    main()
