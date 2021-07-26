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

- Any other files needed for the project: `docker-compose.yml`, shell script used `gameapi.py`, `ab.sh`, `write_hive_table.py`, `proj3_createKafkaTopic.sh`

- Repo explained in the `README.md`.


## Data

Data is generated as events through Apache Bench tests sent as HTTP requests to our `gameapi.py` flask app. The data is published to a kafka topic.

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
  - writes them to storage
- presto then queries those events

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
In a new terminal, set up kafka cat to watch the output streaming on the kafka topic:

```bash
docker-compose exec mids kafkacat -C -b kafka:29092 -t events -o beginning
```
 Inside the mids container we use kafka cat to watch the port for kafka on the events topic from the beginning. We can see the output jsons for transactions being printed to the terminal. These are what will be sent as data rows to Spark.


In a new terminal, navigate to project folder and run apache workbench HTTP requests to flask app through bash script:

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

We use a Spark Python script `write_hive_table.py` to consume data from our kafka topic `events`, filter the data into tables, and save the tables in Cloudera HDFS as parquet files that can be queried using Presto. This is the command to do so:

```bash
docker-compose exec spark spark-submit /w205/project-3-Anand-Patel-95/write_hive_table.py
```
We execute spark's `spark-submit` tool and specify the path of our script. We will see spark execute in the terminal.

### Explaining our Spark Script

As in the previous project, we create a spark session and subscribe to our kafka topic `events` using the kafka bootstrap_servers with the kafka container port specified. We read messages from beginning to end.

In our Spark Session, we enable Hive Support to enable us to store tables and schema into the Hive metastore.

``` Python
spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .enableHiveSupport() \
        .getOrCreate()
```

- Hive is a full on query engine, we don't use it any longer b/c it's slow, but we use the schema registry.
- The hive metastore is friendly with multiple partitions being stored on the file server, and everything that talks to hadoop can talk to the hive metastore.
- We write to it with Spark and we want to read it with Presto, to get them to agree we track the schema with hive metastore.

The method we use to write to hive involves running another Spark job to start up Pyspark and registering a temp table, creating an external table for it, storing it to parquet, and either allowing it to infer a schema or setting it.

#### Functions to filter purchase & guild events:

```Python
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
boolean user defined function, checks if the event json's `event_type` field contains the key word for purchase or guild and returns true. Used in conjunction with:

``` python
purchase_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_purchase('raw'))
```
To take our raw_events, cast as string, and filter only those that return true for `is_purchase`.

Show the dataframe's schema and rows in Spark:

```Python
extracted_guild_events.printSchema()
extracted_guild_events.show()
```

```bash
root
 |-- Accept: string (nullable = true)
 |-- Host: string (nullable = true)
 |-- User-Agent: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- guild_member_cnt: long (nullable = true)
 |-- guild_name: string (nullable = true)
 |-- timestamp: string (nullable = true)
 |-- tot_guild_member_cnt: long (nullable = true)
```


```bash
+------+-----------------+---------------+-----------+----------------+-------------+--------------------+--------------------+
|Accept|             Host|     User-Agent| event_type|guild_member_cnt|   guild_name|           timestamp|tot_guild_member_cnt|
+------+-----------------+---------------+-----------+----------------+-------------+--------------------+--------------------+
|   */*|user1.comcast.com|ApacheBench/2.3| join_guild|               1|  house_stark|2021-07-25 21:56:...|                   1|
|   */*|user1.comcast.com|ApacheBench/2.3| join_guild|               2|  house_stark|2021-07-25 21:56:...|                   2|
|   */*|user1.comcast.com|ApacheBench/2.3| join_guild|               3|  house_stark|2021-07-25 21:56:...|                   3|
|   */*|user1.comcast.com|ApacheBench/2.3| join_guild|               4|  house_stark|2021-07-25 21:56:...|                   4|
|   */*|user1.comcast.com|ApacheBench/2.3|leave_guild|               3|  house_stark|2021-07-25 21:56:...|                   3|
|   */*|user1.comcast.com|ApacheBench/2.3|leave_guild|               2|  house_stark|2021-07-25 21:56:...|                   2|
|   */*|user1.comcast.com|ApacheBench/2.3|leave_guild|               1|  house_stark|2021-07-25 21:56:...|                   1|
|   */*|anand.comcast.com|ApacheBench/2.3| join_guild|               1|        grubs|2021-07-25 21:56:...|                   2|
|   */*|anand.comcast.com|ApacheBench/2.3| join_guild|               2|        grubs|2021-07-25 21:56:...|                   3|
|   */*|  dre.comcast.com|ApacheBench/2.3| join_guild|               3|        grubs|2021-07-25 21:56:...|                   4|
|   */*|  dre.comcast.com|ApacheBench/2.3| join_guild|               1|  secret_club|2021-07-25 21:56:...|                   5|
|   */*|anand.comcast.com|ApacheBench/2.3| join_guild|               2|  secret_club|2021-07-25 21:56:...|                   6|
|   */*|user1.comcast.com|ApacheBench/2.3|leave_guild|            null|house_greyjoy|2021-07-25 21:56:...|                   6|
|   */*|user1.comcast.com|ApacheBench/2.3|leave_guild|            null|house_greyjoy|2021-07-25 21:56:...|                   6|
|   */*|user1.comcast.com|ApacheBench/2.3|leave_guild|            null|house_greyjoy|2021-07-25 21:56:...|                   6|
+------+-----------------+---------------+-----------+----------------+-------------+--------------------+--------------------+
```


#### Send this table's schema to hive and store this table into HDFS as parquet. Will query it with presto.

```Python
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
```

Register the filtered spark dataframe as a temp table.

Use spark sql to drop the table if it already exists, effectively overwriting any existing purchases table in hdfs.

Create an external table purchases if it does not exist. Store is as parquet, store it at `tmp/purchases` in hdfs. Contents will be everything selected from our temp table.

#### Check if our table exists in hdfs.

```bash
docker-compose exec cloudera hadoop fs -ls /tmp/
```

```bash
Found 5 items
drwxr-xr-x   - root   supergroup          0 2021-07-25 22:05 /tmp/guilds
drwxrwxrwt   - mapred mapred              0 2016-04-06 02:26 /tmp/hadoop-yarn
drwx-wx-wx   - hive   supergroup          0 2021-07-25 22:04 /tmp/hive
drwxrwxrwt   - mapred hadoop              0 2016-04-06 02:28 /tmp/logs
drwxr-xr-x   - root   supergroup          0 2021-07-25 22:04 /tmp/purchases
```

```bash
docker-compose exec cloudera hadoop fs -ls -h /tmp/purchases/
```

```bash
-rw-r--r--   1 root supergroup          0 2021-07-25 22:04 /tmp/purchases/_SUCCESS
-rw-r--r--   1 root supergroup      2.1 K 2021-07-25 22:04 /tmp/purchases/part-00000-e8da9102-9538-4e00-be6c-572aa2790e30-c000.snappy.parquet
```

See that the query table is stored as a snappy.parquet file format. Its size is given too; 2.1 K.

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
-----------
guilds    
purchases
(2 rows)

Query 20210725_232234_00002_p68su, FINISHED, 1 node
Splits: 2 total, 0 done (0.00%)
0:01 [0 rows, 0B] [0 rows/s, 0B/s]
```

Get the schema of purchases table
``` presto
describe purchases;
```

``` bash
Column   |  Type   | Comment
------------+---------+---------
accept     | varchar |         
host       | varchar |         
user-agent | varchar |         
cost       | bigint  |         
event_type | varchar |         
timestamp  | varchar |         
type       | varchar |         
(7 rows)

Query 20210725_232417_00003_p68su, FINISHED, 1 node
Splits: 2 total, 0 done (0.00%)
0:01 [0 rows, 0B] [0 rows/s, 0B/s]
```


Get the schema of purchases table
``` presto
describe guilds;
```

``` bash
Column        |  Type   | Comment
----------------------+---------+---------
accept               | varchar |         
host                 | varchar |         
user-agent           | varchar |         
event_type           | varchar |         
guild_member_cnt     | bigint  |         
guild_name           | varchar |         
timestamp            | varchar |         
tot_guild_member_cnt | bigint  |         
(8 rows)

Query 20210725_234217_00004_p68su, FINISHED, 1 node
Splits: 2 total, 1 done (50.00%)
0:00 [8 rows, 535B] [16 rows/s, 1.06KB/s]
```

``` SQL
select * from purchases;
```

``` bash
accept |       host        |   user-agent    | cost |   event_type    |        timestamp        |      type      
--------+-------------------+-----------------+------+-----------------+-------------------------+----------------
*/*    | user1.comcast.com | ApacheBench/2.3 |    6 | purchase_sword  | 2021-07-25 21:56:23.727 | sabre          
*/*    | user1.comcast.com | ApacheBench/2.3 |    6 | purchase_sword  | 2021-07-25 21:56:23.736 | sabre          
*/*    | user1.comcast.com | ApacheBench/2.3 |   42 | purchase_shield | 2021-07-25 21:56:24.217 | mirror_shield  
*/*    | user2.att.com     | ApacheBench/2.3 |   10 | purchase_sword  | 2021-07-25 21:56:28.15  | katana         
*/*    | user2.att.com     | ApacheBench/2.3 |    7 | purchase_sword  | 2021-07-25 21:56:28.634 | estoc          
*/*    | user2.att.com     | ApacheBench/2.3 |    7 | purchase_sword  | 2021-07-25 21:56:28.647 | estoc          
*/*    | user2.comcast.com | ApacheBench/2.3 |    5 | purchase_shield | 2021-07-25 21:56:29.156 | leather_shield
(7 rows)
```

``` SQL
select distinct host from purchases;
```

Order users by the most number of purchases:

```sql
select distinct host, count(*) as num_purchases from purchases group by distinct host order by count(*) desc;
```

```bash
host        | num_purchases
-------------------+---------------
user1.comcast.com |             3
user2.att.com     |             3
user2.comcast.com |             1
```


Order the users by the most amount of money spent in game:
``` SQL
select distinct host, sum(cost) as money_spent from purchases group by distinct host order by sum(cost) desc;
```

```bash
host        | money_spent
-------------------+-------------
user1.comcast.com |          54
user2.att.com     |          24
user2.comcast.com |           5
(3 rows)
```

What is the most purchased item?
``` SQL
select distinct type, count(*) as num_purchases from purchases group by distinct type order by num_purchases desc;
```

``` bash
type      | num_purchases
----------------+---------------
sabre          |             2
estoc          |             2
mirror_shield  |             1
katana         |             1
leather_shield |             1
(5 rows)
```

Show all guild data:
```SQL
select * from guilds;
```

``` bash
accept |       host        |   user-agent    | event_type  | guild_member_cnt |  guild_name   |        timestamp        | tot_guild_member_cnt
--------+-------------------+-----------------+-------------+------------------+---------------+-------------------------+----------------------
*/*    | user1.comcast.com | ApacheBench/2.3 | join_guild  |                1 | house_stark   | 2021-07-25 21:56:24.7   |                    1
*/*    | user1.comcast.com | ApacheBench/2.3 | join_guild  |                2 | house_stark   | 2021-07-25 21:56:24.71  |                    2
*/*    | user1.comcast.com | ApacheBench/2.3 | join_guild  |                3 | house_stark   | 2021-07-25 21:56:24.719 |                    3
*/*    | user1.comcast.com | ApacheBench/2.3 | join_guild  |                4 | house_stark   | 2021-07-25 21:56:24.731 |                    4
*/*    | user1.comcast.com | ApacheBench/2.3 | leave_guild |                3 | house_stark   | 2021-07-25 21:56:25.2   |                    3
*/*    | user1.comcast.com | ApacheBench/2.3 | leave_guild |                2 | house_stark   | 2021-07-25 21:56:25.208 |                    2
*/*    | user1.comcast.com | ApacheBench/2.3 | leave_guild |                1 | house_stark   | 2021-07-25 21:56:25.213 |                    1
*/*    | anand.comcast.com | ApacheBench/2.3 | join_guild  |                1 | grubs         | 2021-07-25 21:56:25.729 |                    2
*/*    | anand.comcast.com | ApacheBench/2.3 | join_guild  |                2 | grubs         | 2021-07-25 21:56:25.739 |                    3
*/*    | dre.comcast.com   | ApacheBench/2.3 | join_guild  |                3 | grubs         | 2021-07-25 21:56:26.223 |                    4
*/*    | dre.comcast.com   | ApacheBench/2.3 | join_guild  |                1 | secret_club   | 2021-07-25 21:56:26.694 |                    5
*/*    | anand.comcast.com | ApacheBench/2.3 | join_guild  |                2 | secret_club   | 2021-07-25 21:56:27.198 |                    6
*/*    | user1.comcast.com | ApacheBench/2.3 | leave_guild | NULL             | house_greyjoy | 2021-07-25 21:56:27.661 |                    6
*/*    | user1.comcast.com | ApacheBench/2.3 | leave_guild | NULL             | house_greyjoy | 2021-07-25 21:56:27.669 |                    6
*/*    | user1.comcast.com | ApacheBench/2.3 | leave_guild | NULL             | house_greyjoy | 2021-07-25 21:56:27.675 |                    6
(15 rows)
```

Table without the Nulls:

``` sql
select * from guilds where guild_member_cnt is not NULL;
```

``` bash
accept |       host        |   user-agent    | event_type  | guild_member_cnt | guild_name  |        timestamp        | tot_guild_member_cnt
--------+-------------------+-----------------+-------------+------------------+-------------+-------------------------+----------------------
*/*    | user1.comcast.com | ApacheBench/2.3 | join_guild  |                1 | house_stark | 2021-07-25 21:56:24.7   |                    1
*/*    | user1.comcast.com | ApacheBench/2.3 | join_guild  |                2 | house_stark | 2021-07-25 21:56:24.71  |                    2
*/*    | user1.comcast.com | ApacheBench/2.3 | join_guild  |                3 | house_stark | 2021-07-25 21:56:24.719 |                    3
*/*    | user1.comcast.com | ApacheBench/2.3 | join_guild  |                4 | house_stark | 2021-07-25 21:56:24.731 |                    4
*/*    | user1.comcast.com | ApacheBench/2.3 | leave_guild |                3 | house_stark | 2021-07-25 21:56:25.2   |                    3
*/*    | user1.comcast.com | ApacheBench/2.3 | leave_guild |                2 | house_stark | 2021-07-25 21:56:25.208 |                    2
*/*    | user1.comcast.com | ApacheBench/2.3 | leave_guild |                1 | house_stark | 2021-07-25 21:56:25.213 |                    1
*/*    | anand.comcast.com | ApacheBench/2.3 | join_guild  |                1 | grubs       | 2021-07-25 21:56:25.729 |                    2
*/*    | anand.comcast.com | ApacheBench/2.3 | join_guild  |                2 | grubs       | 2021-07-25 21:56:25.739 |                    3
*/*    | dre.comcast.com   | ApacheBench/2.3 | join_guild  |                3 | grubs       | 2021-07-25 21:56:26.223 |                    4
*/*    | dre.comcast.com   | ApacheBench/2.3 | join_guild  |                1 | secret_club | 2021-07-25 21:56:26.694 |                    5
*/*    | anand.comcast.com | ApacheBench/2.3 | join_guild  |                2 | secret_club | 2021-07-25 21:56:27.198 |                    6
(12 rows)
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
grubs       |                3
secret_club |                2
house_stark |                1
(3 rows)
```

Which players are join the most guilds?

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

Which players are leave the most guilds?

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
