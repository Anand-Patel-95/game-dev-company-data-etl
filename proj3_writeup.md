## Anand Patel
# Project 3: Understanding User Behavior

- You're a data scientist at a game development company

- Your latest mobile game has at least two events you're interested in tracking: buy a sword & join guild

- Each has metadata characterstic of such events (i.e., sword type, guild name, etc)

Implement a data pipeline that does the following:

- Assemble a data pipeline to catch these events: use Spark streaming to filter select event types from Kafka, land them into HDFS/parquet to make them available for analysis using Presto.

- Use Apache Bench to generate test data for your pipeline.

- Produce an analytics report where you provide a description of your pipeline and some basic analysis of the events.

## Deliverables

- history file: `Anand-Patel-history.txt`

- Report files: `proj3_writeup.md`.

- Any other files needed for the project: `docker-compose.yml`, scripts used:
  - `ab.sh`: bash script to send apache bench test data to spark for batch mode.
  - `ab2.sh`: bash script to send apache bench test data to spark for streaming mode.
  - `proj3_createKafkaTopic.sh`: creates a kafka `events` topic.
  - `write_hive_table.py`: spark script to send data from events topic to presto in batch mode.
  - `stream_and_hive.py`: spark script to send data from events topic to presto in streaming mode.
  - `game_api.py`: our flask game app

- Repo explained in the `README.md`.


## Data

Data is generated as events through Apache Bench tests sent as HTTP requests to our `game_api.py` flask app. The data is published to a kafka topic.

## ETL Pipeline Summary

We use `Apache Bench` to generate test data to simulate users buying items, joining or leaving guilds, and other gameplay events. These requests are sent to our `Flask App` to generate our event logs. Our event logs are published to `Kafka`, our queue. We use `Spark` for stream ingestion from our queue and filtering our event data. We use `Cloudera` as our Hadoop File System for distributed cloud storage, and our tables will be stored as parquet files. We enable our data in HDFS to be inspected by data scientists through the Query tool `Presto`.  

- user interacts with mobile app
- mobile app makes API calls to web services
- API server handles requests:
  - handles actual business requirements (e.g., process purchase)
  - logs events to kafka
- spark then:
  - pulls events from kafka
  - filters/flattens/transforms events
  - writes them to storage as tables, and table schema is communicated through HIVE_THRIFTSERVER
  - in streaming mode: spark continuously sinks data into cloudera
- presto then queries those events from tables saved by spark

## New `Docker-Compose.yml`:

``` yaml
---
version: '2'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 32181
      ZOOKEEPER_TICK_TIME: 2000
    expose:
      - "2181"
      - "2888"
      - "32181"
      - "3888"
    extra_hosts:
      - "moby:127.0.0.1"

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:32181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    expose:
      - "9092"
      - "29092"
    extra_hosts:
      - "moby:127.0.0.1"

  cloudera:
    image: midsw205/hadoop:0.0.2
    hostname: cloudera
    expose:
      - "8020" # nn
      - "8888" # hue
      - "9083" # hive thrift
      - "10000" # hive jdbc
      - "50070" # nn http
    ports:
      - "8888:8888" # has port option enabled so we can use Hue: graphical user interface for SQL databases.
    extra_hosts:
      - "moby:127.0.0.1"

  spark:
    image: midsw205/spark-python:0.0.6
    stdin_open: true
    tty: true
    volumes:
      - ~/w205:/w205
    expose:
      - "8888"
    #ports:
    #  - "8888:8888"  # commented this out b/c we're listening to 8888 above too. If this was in we would have to 1) change port number on host side left or 2) comment this out
    depends_on:
      - cloudera
    environment:
      HADOOP_NAMENODE: cloudera
      HIVE_THRIFTSERVER: cloudera:9083
    extra_hosts:
      - "moby:127.0.0.1"
    command: bash

  presto:
    image: midsw205/presto:0.0.1
    hostname: presto
    volumes:
      - ~/w205:/w205
    expose:
      - "8080"
    environment:
      HIVE_THRIFTSERVER: cloudera:9083 # needs connection to HIVE_THRIFTSERVER
    extra_hosts:
      - "moby:127.0.0.1"

  mids:
    image: midsw205/base:0.1.9
    stdin_open: true
    tty: true
    volumes:
      - ~/w205:/w205
    expose:
      - "5000"
    ports:
      - "5000:5000" # listening here b/c default port for flask framework
    extra_hosts:
      - "moby:127.0.0.1"
```

In the docker-compose file, we add containers for:

- **presto**: Used to run queries on our parquet files saved into HDFS cloudera. Presto does not include built in support for the Hadoop file system and it will need to leverage other tools such as Hive connector. We connect to a HIVE_THRIFTSERVER.
- **cloudera**: We expose ports for hive thrift, hive jdbc, and hue (a graphical ui that I do not use).
- **spark**: Add dependency for hadoop. Add environment parameters for the hadoop namenode and for the HIVE_THRIFTSERVER using the post specified in cloudera's container.
- **mids**: Added listening to port for flask app.

```
docker-compose up -d
```


## Launching Kafka

Create kafka topic:

``` bash
 bash proj3_createKafkaTopic.sh
```

Bash script contains:
```bash
docker-compose exec kafka kafka-topics --create --topic events --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:32181
```

## Launch Flask app:

```bash
docker-compose exec mids env FLASK_APP=/w205/project-3-Anand-Patel-95/game_api.py flask run --host 0.0.0.0
```
We tell `docker-compose` to execute from the `mids` container the `FLASK_APP` located at `/w205/project-3-Anand-Patel-95/game_api.py` and the `0.0.0.0` means we can send requests from outside the cloud instance too.

### `game_api.py` Flask app:

``` python
#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')
events_topic = 'events'

tot_guild_member_cnt = 0   # count of total number of guild members
sword_names = {'copper_sword': 4, 'iron_sword' : 5, 'long_sword': 9, 'rapier': 7, 'short_sword': 5, 'master_sword': 77,
              'scimatar': 8, 'katana': 10, 'hook_sword': 4, 'machete': 5, 'falcion': 5, 'sabre': 6, 'great_sword': 15,
              'moonlight_great_sword': 75, 'wood_sword': 1, 'estoc': 7}

shield_names = {'great_shield': 14, 'buckler' : 6, 'mirror_shield': 42, 'spiked_shield': 9, 'golden_shield': 40, 'wood_shield': 3,
              'leather_shield': 5, "copper_shield" : 4, "iron_shield": 7}

guilds_dict = {
    "house_stark" : {"member_cnt" : 0},
    "grubs" : {"member_cnt" : 0},
    "ucbears" : {"member_cnt" : 0},
    "joestars" : {"member_cnt" : 0},
    "winden_caves" : {"member_cnt" : 0}
}

def log_to_kafka(topic, event):
    event.update(request.headers) # gives us info coming from HTTP headers
    producer.send(topic, json.dumps(event).encode())


@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"


@app.route("/purchase_a_sword/<sword_name>")
def purchase_a_sword(sword_name):
    # check if sword_name is in predefined dictionary
    sword_cost = sword_names.get(sword_name, None)

    purchase_sword_event = {'event_type': 'purchase_sword', 'type' : sword_name, 'cost': sword_cost}
    log_to_kafka('events', purchase_sword_event)
    return "Sword Purchased! " + sword_name +  "\n"


@app.route("/purchase_a_shield/<shield_name>")
def purchase_a_shield(shield_name):
    # check if shield_name is in predefined dictionary
    shield_cost = shield_names.get(shield_name, None)

    purchase_shield_event = {'event_type': 'purchase_shield', 'type' : shield_name, 'cost': shield_cost}
    log_to_kafka('events', purchase_shield_event)
    return "Shield Purchased! " + shield_name +  "\n"


@app.route("/join_guild/<guild_name>")
def join_guild(guild_name):
    # business logic to join guild
    global guilds_dict
    global tot_guild_member_cnt
    tot_guild_member_cnt += 1

    # check if guild name exists, otherwise create it
    if guild_name in guilds_dict:
        # increment the number of members in this guild
        guilds_dict[guild_name]["member_cnt"] += 1
    else:
        # create this guild, add this player to it
        guilds_dict[guild_name] = {"member_cnt" : 1}

    # log event to kafka    
    join_guild_event = {'event_type': 'join_guild', 'tot_guild_member_cnt': tot_guild_member_cnt, 'guild_name': guild_name, 'guild_member_cnt': guilds_dict[guild_name]["member_cnt"]}
    log_to_kafka('events', join_guild_event)

    return_str = "Joined Guild: " + guild_name + "! Total members:" + str(guilds_dict[guild_name]["member_cnt"]) + "\n"
    return return_str

@app.route("/leave_guild/<guild_name>")
def leave_guild(guild_name):
    # business logic to join guild
    global guilds_dict
    global tot_guild_member_cnt

    if guild_name in guilds_dict:
        # decrement the number of members in this guild
        guilds_dict[guild_name]["member_cnt"] -= 1
        if guilds_dict[guild_name]["member_cnt"] < 0:
            guilds_dict[guild_name]["member_cnt"] = 0

        ending_guild_mem_cnt = guilds_dict[guild_name]["member_cnt"]
        # decrement total guild member count
        tot_guild_member_cnt -= 1
        if tot_guild_member_cnt < 0:
            tot_guild_member_cnt = 0
        return_str = "Left Guild: "+ guild_name + "! Total members:" + str(ending_guild_mem_cnt) + "\n"
    else:
        # guild does not exist this. Do nothing
        return_str = "Can not leave Guild: "+ guild_name + "! This guild does not exist.\n"
        ending_guild_mem_cnt = None


    # log event to kafka    
    leave_guild_event = {'event_type': 'leave_guild', 'tot_guild_member_cnt': tot_guild_member_cnt, 'guild_name': guild_name, 'guild_member_cnt': ending_guild_mem_cnt}
    log_to_kafka('events', leave_guild_event)

    return return_str
```


Above is the flask app `game_api.py`. It uses HTTP get commands and supports the following functionalities:
- Default response: Does nothing.
- `/purchase_a_sword/<sword_name>`: which lets a user send a request to purchase a specified type of sword. Sword types are predefined in the dictionary `sword_names` but this could be saved in a configuration file somewhere. Swords have a cost associated, which is reported back if the sword was bought or a null is returned.
- `/purchase_a_sword/<shield_name>`: functions the same as swords, lets a user buy a shield from our game.
- `/join_guild/<guild_name>`: Allows a user to join a specified guild or create that guild and join it. Returns the name of the guild and the number of members in this guild and the number of total players in guilds.
- `/leave_guild/<guild_name>`: Allows a user to leave a specified guild. If the guild exists, its member count is decreased by 1 and returned. We also return the guild name and total guild member count. If the guild does not exist, we return `null` for this guild's member count.

The flask app sets up a `KafkaProducer` on our kafka container's port number. We publish data to the kafka topic `events`. We add to our events log json (our data for this transaction) by adding our HTTP request headers. This is done in the function `log_to_kafka`.

## Sending Flask commands
In a new terminal, set up `kafkacat` to watch the output streaming on the kafka topic:

```bash
docker-compose exec mids kafkacat -C -b kafka:29092 -t events -o beginning
```
Inside the mids container we use kafka cat to watch the port for kafka on the events topic from the beginning. We can see the output jsons for transactions being printed to the terminal. These are what will be sent as data rows to Spark.

#### Using Apache Bench for sending HTTP test queries

In a new terminal, navigate to project folder and run apache workbench HTTP requests to flask app through bash script:

In batch mode:

```bash
bash ab.sh
```

Sample of some of the commands sent:
```bash
docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/
docker-compose exec mids ab -n 2 -H "Host: user1.comcast.com" http://localhost:5000/purchase_a_sword/sabre
docker-compose exec mids ab -n 1 -H "Host: user1.comcast.com" http://localhost:5000/purchase_a_shield/mirror_shield
docker-compose exec mids ab -n 4 -H "Host: user1.comcast.com" http://localhost:5000/join_guild/house_stark
docker-compose exec mids ab -n 3 -H "Host: user1.comcast.com" http://localhost:5000/leave_guild/house_stark
docker-compose exec mids ab -n 2 -H "Host: anand.comcast.com" http://localhost:5000/join_guild/grubs
docker-compose exec mids ab -n 1 -H "Host: dre.comcast.com" http://localhost:5000/join_guild/grubs
docker-compose exec mids ab -n 1 -H "Host: dre.comcast.com" http://localhost:5000/join_guild/secret_club
docker-compose exec mids ab -n 1 -H "Host: anand.comcast.com" http://localhost:5000/join_guild/secret_club
docker-compose exec mids ab -n 3 -H "Host: user1.comcast.com" http://localhost:5000/leave_guild/house_greyjoy
docker-compose exec mids ab -n 1 -H "Host: user2.att.com" http://localhost:5000/purchase_a_sword/katana
docker-compose exec mids ab -n 2 -H "Host: user2.att.com" http://localhost:5000/purchase_a_sword/estoc
docker-compose exec mids ab -n 1 -H "Host: user2.comcast.com" http://localhost:5000/purchase_a_shield/leather_shield
```

We can see that this runs Apache Bench `ab` commands through the docker-compose `mids` container. `-n 1` specifies we want to run this 1 times. `-H "Host: anand.comcast.com"` specifies the host name for this command. Finally, we have the url for the HTTP command to the flask app. Apache Bench will repeat a command however many times you specified, and will show metrics on the command like the time taken for tests, failed tests, bytes transferred, etc.

**We use streaming mode**, so our bash script run is `ab2.sh`, which puts the commands above into a loop:

``` bash
while true; do
  docker-compose exec mids ab -n 2 -H "Host: user1.comcast.com" http://localhost:5000/purchase_a_sword/sabre
  docker-compose exec mids ab -n 1 -H "Host: user1.comcast.com" http://localhost:5000/purchase_a_shield/mirror_shield
  docker-compose exec mids ab -n 4 -H "Host: user1.comcast.com" http://localhost:5000/join_guild/house_stark
  docker-compose exec mids ab -n 3 -H "Host: user1.comcast.com" http://localhost:5000/leave_guild/house_stark
  docker-compose exec mids ab -n 2 -H "Host: anand.comcast.com" http://localhost:5000/join_guild/grubs
  docker-compose exec mids ab -n 1 -H "Host: dre.comcast.com" http://localhost:5000/join_guild/grubs
  docker-compose exec mids ab -n 1 -H "Host: dre.comcast.com" http://localhost:5000/join_guild/secret_club
  docker-compose exec mids ab -n 1 -H "Host: anand.comcast.com" http://localhost:5000/join_guild/secret_club
  docker-compose exec mids ab -n 3 -H "Host: user1.comcast.com" http://localhost:5000/leave_guild/house_greyjoy
  docker-compose exec mids ab -n 1 -H "Host: user2.att.com" http://localhost:5000/purchase_a_sword/katana
  docker-compose exec mids ab -n 2 -H "Host: user2.att.com" http://localhost:5000/purchase_a_sword/estoc
  docker-compose exec mids ab -n 1 -H "Host: user2.comcast.com" http://localhost:5000/purchase_a_shield/leather_shield
  sleep 10
done
```

We can add more complex test scripts above to simulate multiple users. Running this bash script will occupy a terminal and send these commands every 10 seconds until the process is killed with `ctrl+C`. In a new terminal, run `bash ab2.sh`.

### See the output in kafkacat

This is a sample of the output for some of the HTTP commands being sent to our flask app. This simulates transaction events for our users.
```bash
{"event_type": "leave_guild", "Accept": "*/*", "User-Agent": "ApacheBench/2.3", "Host": "user1.comcast.com", "guild_member_cnt": null, "tot_guild_member_cnt": 6, "guild_name": "house_greyjoy"}
{"event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3", "Host": "user2.att.com", "cost": 10, "type": "katana"}
{"event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3", "Host": "user2.att.com", "cost": 7, "type": "estoc"}
{"event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3", "Host": "user2.att.com", "cost": 7, "type": "estoc"}
{"event_type": "purchase_shield", "Accept": "*/*", "User-Agent": "ApacheBench/2.3", "Host": "user2.comcast.com", "cost": 5, "type": "leather_shield"}
```

## Using Spark to consume data, process, save to HDFS

In batch mode, we use a Spark Python script `write_hive_table.py` to consume data from our kafka topic `events`, filter the data into tables, and save the tables in Cloudera HDFS as parquet files that can be queried using Presto. This is the command to do so:

```bash
docker-compose exec spark spark-submit /w205/project-3-Anand-Patel-95/write_hive_table.py
```
We execute spark's `spark-submit` tool and specify the path of our script. We will see spark execute in the terminal.



**For this project**, we use streaming for Spark and run the following script in a new terminal to consume data from our kafka topic, filter it, and continuously save the tables into Cloudera HDFS as parquet files that we can query with Presto:

```Bash
docker-compose exec spark spark-submit /w205/project-3-Anand-Patel-95/stream_and_hive.py
```

Since the streaming of data is continuous, we will need to open a new terminal to keep this spark stream script running. We `ctrl+C` to break out of it when we want to stop sending data into HDFS.

### Explaining our Streaming and Hive Spark Script: `stream_and_hive.py`


``` Python
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
    # here we hand over the schema explicitly, last time we registered a temp table
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
```


As in the previous project, we create a spark session and subscribe to our kafka topic `events` using the kafka bootstrap_servers with the kafka container port specified. We read messages from a stream using `readStream` this time instead of beginning to end.

``` Python
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
```

In our Spark Session, we enable Hive Support to enable us to store tables and schema into the Hive metastore.

- Hive is a full on query engine, we don't use it any longer b/c it's slow, but we use the schema registry.
- The hive metastore is friendly with multiple partitions being stored on the file server, and everything that talks to hadoop can talk to the hive metastore.
- We write to it with Spark and we want to read it with Presto, to get them to agree we track the schema with hive metastore.


#### Functions to define event schema:

These functions are used to return an event's user defined schema and the datatypes associated. This allows us to enforce a schema on our filtered data.

Here is an example of a schema for the purchase events:

``` python
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
```


#### Functions to filter purchase & guild events:

```Python
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
```

Boolean user defined function, checks if the event json's `event_type` field contains the key word for purchase or guild and returns true. We use these functions to filter our raw data into separate tables.

#### Creating a spark dataframe from raw data in streaming mode

Here is an example from our script of creating a data frame for extracted_purchase_events.

``` python
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
```

First we filter our raw data for rows that are purchase events by passing the string casted value of raw_events into our user defined boolean function.

Then we select the following for rows:
- the `raw_events.value` casted as a string as column name 'raw_event'
- the `raw_events.timestamp` casted as a string
- the extracted fields of the json of `raw_events.value` based on our defined schema and types.

We can print the schema of the extracted_purchase_events to the terminal that spark is running in. Note that in streaming mode, we cannot show the contents of extracted_purchase_events on the terminal.

#### Write the table to HDFS, Hive metastore

Next, we writeStream the dataframe into cloudera as tables stored as parquet files. The method we use to write to cloudera and hive metastore (for the schema) involves running another Spark job to start up Pyspark and using spark sql to create an external table for our dataframe, storing it to parquet, specifying the schema and selected columns, and giving a processing time for the sink to periodically write to stream.

```Python
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
```

First, we drop the table `purchase` if one already exists.

The spark sql string & execution creates an external table named `purchases` if one does not exist. It has the specified columns and datatypes. Note that we needed to use the escape characters for `User-Agent`. We store this table as parquet in the specified location in HDFS. We give the table the property to do SNAPPY compress for the parquet files.

We define a sink for `extracted_purchase_events` being written as a stream. We specify the format as parquet, give a location for the checkpoint, give the path for the table, give the processingTime as 10 seconds for writing to HDFS, and starting the sink.

In this script, we write 3 tables in streaming mode so we have 3 sinks or query. The following line of code goes after the last sink in our script and will block until any one of our streams terminates. This will let our spark session wait until any of the queries on the associated SQLContext has terminated since the creation of the context, or since resetTerminated() was called. We use this instead of `query.awaitTermination()`. `spark` in the code below is the name of our spark session.

```python
spark.streams.awaitAnyTermination()  # https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
```

After a couple cycles of the Spark script and our Apache Bench bash script, we should have streamed data into HDFS as 3 tables for `sword_purchases`, `purchases`, and `guilds`. We can keep both of these scripts running in their terminals to keep generating data.

## Check if our tables exist in HDFS.

```bash
docker-compose exec cloudera hadoop fs -ls /tmp/
```

```bash
Found 9 items
drwxrwxrwt   - root   supergroup          0 2021-07-30 18:35 /tmp/checkpoints_for_guild_events
drwxrwxrwt   - root   supergroup          0 2021-07-30 18:35 /tmp/checkpoints_for_purchase_events
drwxrwxrwt   - root   supergroup          0 2021-07-30 18:35 /tmp/checkpoints_for_sword_purchases
drwxrwxrwt   - root   supergroup          0 2021-07-30 18:51 /tmp/guilds
drwxrwxrwt   - mapred mapred              0 2016-04-06 02:26 /tmp/hadoop-yarn
drwx-wx-wx   - hive   supergroup          0 2021-07-30 18:35 /tmp/hive
drwxrwxrwt   - mapred hadoop              0 2016-04-06 02:28 /tmp/logs
drwxrwxrwt   - root   supergroup          0 2021-07-30 18:51 /tmp/purchases
drwxrwxrwt   - root   supergroup          0 2021-07-30 18:51 /tmp/sword_purchases
```

We can see our tables for `sword_purchases`, `purchases`, and `guilds` exist.

```bash
docker-compose exec cloudera hadoop fs -ls -h /tmp/purchases/
```

```bash
drwxr-xr-x   - root supergroup          0 2021-07-30 18:51 /tmp/purchases/_spark_metadata
-rw-r--r--   1 root supergroup      2.8 K 2021-07-30 18:40 /tmp/purchases/part-00000-0057d1e6-cf93-4193-af80-3c4eb215d3c0-c000.snappy.parquet
-rw-r--r--   1 root supergroup      2.9 K 2021-07-30 18:41 /tmp/purchases/part-00000-03945cc1-3113-4e87-be1b-aad508e580b8-c000.snappy.parquet
-rw-r--r--   1 root supergroup      3.1 K 2021-07-30 18:37 /tmp/purchases/part-00000-0570da1c-7735-4a2d-a265-cf03799f455a-c000.snappy.parquet
-rw-r--r--   1 root supergroup      3.1 K 2021-07-30 18:49 /tmp/purchases/part-00000-0ba632a2-2d1b-4a53-8d65-cc11f17efd72-c000.snappy.parquet
-rw-r--r--   1 root supergroup      2.9 K 2021-07-30 18:47 /tmp/purchases/part-00000-0be76fe6-c6ef-4a79-90e2-41a8c2ec67c6-c000.snappy.parquet
-rw-r--r--   1 root supergroup      3.1 K 2021-07-30 18:48 /tmp/purchases/part-00000-14081c70-42d7-4f98-ac7b-1efbfc7bbab0-c000.snappy.parquet
-rw-r--r--   1 root supergroup      2.9 K 2021-07-30 18:42 /tmp/purchases/part-00000-171f7ee9-bc2b-47fb-8c39-0948d50f7aab-c000.snappy.parquet
-rw-r--r--   1 root supergroup      2.9 K 2021-07-30 18:41 /tmp/purchases/part-00000-17718941-1470-44b6-a65f-825fd0f70647-c000.snappy.parquet
-rw-r--r--   1 root supergroup      3.1 K 2021-07-30 18:50 /tmp/purchases/part-00000-19c66051-64c7-45fa-8417-a07da5c07ccf-c000.snappy.parquet
-rw-r--r--   1 root supergroup      2.9 K 2021-07-30 18:45 /tmp/purchases/part-00000-1b641d8c-f951-4e5e-81d9-2bd79553c623-c000.snappy.parquet
-rw-r--r--   1 root supergroup      2.7 K 2021-07-30 18:51 /tmp/purchases/part-00000-1ebc077c-ac12-45d6-b211-c8c798b090da-c000.snappy.parquet
-rw-r--r--   1 root supergroup      2.9 K 2021-07-30 18:48 /tmp/purchases/part-00000-1ff0c21c-684c-4831-b799-a132a17fc1e7-c000.snappy.parquet
-rw-r--r--   1 root supergroup      3.0 K 2021-07-30 18:43 /tmp/purchases/part-00000-209d76ca-b2b0-4655-8409-f50a4ca503ed-c000.snappy.parquet
-rw-r--r--   1 root supergroup      2.9 K 2021-07-30 18:42 /tmp/purchases/part-00000-215d25ec-a655-4a28-a415-f4c2016c3a94-c000.snappy.parquet
-rw-r--r--   1 root supergroup      2.9 K 2021-07-30 18:38 /tmp/purchases/part-00000-2164ecc3-33c2-4436-a6cd-41234be75fcf-c000.snappy.parquet
-rw-r--r--   1 root supergroup      3.0 K 2021-07-30 18:48 /tmp/purchases/part-00000-22686716-d0c7-4306-9c8c-49e694a44cea-c000.snappy.parquet
-rw-r--r--   1 root supergroup      2.7 K 2021-07-30 18:35 /tmp/purchases/part-00000-253d8359-a162-4398-8a90-db3826ed371a-c000.snappy.parquet
-rw-r--r--   1 root supergroup      2.9 K 2021-07-30 18:49 /tmp/purchases/part-00000-27062dd0-257c-4e3f-9d5e-2961bc528d91-c000.snappy.parquet
```

See that the query tables are stored as a snappy.parquet file format. We have multiple tables stored since we are running in streaming mode. Each table is stored as a small file by the HDFS algorithm.

## Launch Presto for Queries

- Presto is a query engine.
- It's talking to the hive thrift server to get the table we just added.
- Connected to hdfs to get the data.
- Querying with presto instead of spark because presto scales well, handles a wider range of sql syntax, can start treating like a database, can configure it to talk to cassandra, s3 directly, kafka directly, mysql, and is a good front end for your company's data.

``` bash
docker-compose exec presto presto --server presto:8080 --catalog hive --schema default
```

Above we execute presto, tool presto. Give the exposed port number for presto from the docker-compose file. Use hive for the catalog and default for the schema.

Show the tables available in presto.

``` presto
show tables;
```
``` bash
Table      
-----------------
guilds          
purchases       
sword_purchases
(3 rows)

Query 20210730_185430_00001_xkyps, FINISHED, 1 node
Splits: 2 total, 0 done (0.00%)
0:03 [0 rows, 0B] [0 rows/s, 0B/s]
```
The purchases table will contain the sword purchases and shield purchases.

Get the schema of `purchases` table:

``` presto
describe purchases;
```

``` bash
   Column   |  Type   | Comment
------------+---------+---------
 raw_event  | varchar |         
 timestamp  | varchar |         
 accept     | varchar |         
 host       | varchar |         
 user-agent | varchar |         
 event_type | varchar |         
 type       | varchar |         
 cost       | bigint  |         
(8 rows)

Query 20210730_185500_00003_xkyps, FINISHED, 1 node
Splits: 2 total, 0 done (0.00%)
0:01 [0 rows, 0B] [0 rows/s, 0B/s]
```


##### Questions about Purchases

``` SQL
select * from purchases limit 5;
```

```
raw_event                                                                       |        timestamp        | accept |       host
-------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------+--------+------------
{"event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3", "Host": "user1.comcast.com", "cost": 6, "type": "sabre"}           | 2021-07-30 18:43:03.611 | */*    | user1.comca
{"event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3", "Host": "user1.comcast.com", "cost": 6, "type": "sabre"}           | 2021-07-30 18:43:03.622 | */*    | user1.comca
{"event_type": "purchase_shield", "Accept": "*/*", "User-Agent": "ApacheBench/2.3", "Host": "user1.comcast.com", "cost": 42, "type": "mirror_shield"} | 2021-07-30 18:43:04.268 | */*    | user1.comca
{"event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3", "Host": "user2.att.com", "cost": 10, "type": "katana"}             | 2021-07-30 18:43:09.496 | */*    | user2.att.c
{"event_type": "purchase_sword", "Accept": "*/*", "User-Agent": "ApacheBench/2.3", "Host": "user1.comcast.com", "cost": 6, "type": "sabre"}           | 2021-07-30 18:46:52.785 | */*    | user1.comca

|        timestamp        | accept |       host        |   user-agent    |   event_type    |     type      | cost
----------------------------------------------------+-------------------------+--------+-------------------+-----------------+-----------------+---------------+------
comcast.com", "cost": 6, "type": "sabre"}           | 2021-07-30 18:43:03.611 | */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword  | sabre         |    6
comcast.com", "cost": 6, "type": "sabre"}           | 2021-07-30 18:43:03.622 | */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword  | sabre         |    6
.comcast.com", "cost": 42, "type": "mirror_shield"} | 2021-07-30 18:43:04.268 | */*    | user1.comcast.com | ApacheBench/2.3 | purchase_shield | mirror_shield |   42
att.com", "cost": 10, "type": "katana"}             | 2021-07-30 18:43:09.496 | */*    | user2.att.com     | ApacheBench/2.3 | purchase_sword  | katana        |   10
comcast.com", "cost": 6, "type": "sabre"}           | 2021-07-30 18:46:52.785 | */*    | user1.comcast.com | ApacheBench/2.3 | purchase_sword  | sabre         |    6
(5 rows)

```

Get the players who bought something.

``` SQL
select distinct host from purchases;
```

``` bash
host        
-------------------
user1.comcast.com
user2.att.com     
user2.comcast.com
(3 rows)
```

Order players by the most number of purchases:

```sql
select distinct host, count(*) as num_purchases from purchases group by distinct host order by count(*) desc;
```

```bash
host        | num_purchases
-------------------+---------------
user1.comcast.com |           156
user2.att.com     |           156
user2.comcast.com |            52
(3 rows)
```


Order the players by the most amount of money spent in game:

``` SQL
select distinct host, sum(cost) as money_spent from purchases group by distinct host order by sum(cost) desc;
```

```bash
host        | money_spent
-------------------+-------------
user1.comcast.com |        2808
user2.att.com     |        1248
user2.comcast.com |         260
(3 rows)
```

What is the most purchased item?

``` SQL
select distinct type, count(*) as num_purchases from purchases group by distinct type order by num_purchases desc;
```

``` bash
type      | num_purchases
----------------+---------------
sabre          |           104
estoc          |           104
mirror_shield  |            52
katana         |            52
leather_shield |            52
(5 rows)
```

##### Questions about Guilds

The `guilds` table will have events for joining and leaving guilds.

Get the schema of `guilds` table:

```SQL
describe guilds;
```

``` bash
Column        |  Type   | Comment
----------------------+---------+---------
raw_event            | varchar |         
timestamp            | varchar |         
accept               | varchar |         
host                 | varchar |         
user-agent           | varchar |         
event_type           | varchar |         
guild_member_cnt     | bigint  |         
guild_name           | varchar |         
tot_guild_member_cnt | bigint  |         
(9 rows)
```


How many guild events?

```SQL
select count(*) as num_events from guilds;
```

``` bash
num_events
------------
       780
(1 row)
```

Nulls exist in `guild_member_cnt` when a person tried to leave a guild that did not exist.

Size of `guilds` table without the Nulls:

``` sql
select count(*) as num_events from guilds where guild_member_cnt is not NULL;
```

``` bash
num_events
------------
       624
(1 row)
```

Which guilds currently have the most members?

``` SQL
WITH current_guilds AS (
  SELECT m.*, ROW_NUMBER() OVER (PARTITION BY guild_name ORDER BY num_row DESC) AS rn
  FROM (
    SELECT ROW_NUMBER() OVER() AS num_row,
                 guild_name,
                 guild_member_cnt
                 FROM guilds
                 where guild_member_cnt is not NULL
      ) AS m
)
SELECT guild_name, guild_member_cnt FROM current_guilds WHERE rn = 1 ORDER BY guild_member_cnt desc;
```

``` bash
guild_name  | guild_member_cnt
-------------+------------------
grubs       |               90
secret_club |               60
house_stark |               30
(3 rows)
```

Which players join the most guilds?

``` sql
select host, event_type, count(distinct guild_name) as num_times
from (
  select *
  from guilds
  where guild_member_cnt is not NULL)
group by host, event_type
having event_type = 'join_guild'
order by num_times desc;
```

``` bash
host        | event_type | num_times
-------------------+------------+-----------
anand.comcast.com | join_guild |         2
dre.comcast.com   | join_guild |         2
user1.comcast.com | join_guild |         1
(3 rows)
```

Note that we only have this many players joining guilds because the game_api currently does not prevent a member from sending a request to join a guild they are already in since I don't keep track of each player's guilds. We only really have those players above in 1 or 2 unique guilds because our apache bench script `ab2.sh` only tests with 3 guilds by design currently.

Which players leave the most guilds?

``` sql
select host, event_type, count(distinct guild_name) as num_times
from (
  select *
  from guilds
  where guild_member_cnt is not NULL)
group by host, event_type
having event_type = 'leave_guild'
order by num_times desc;
```

``` bash
host        | event_type  | num_times
-------------------+-------------+-----------
user1.comcast.com | leave_guild |         1
```



## Taking down docker

This command downs the containers brought up by docker-compose.

```
docker-compose down
```


---
