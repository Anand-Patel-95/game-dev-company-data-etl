Anand Patel

# Project 3: Understanding User Behavior

- You're a data scientist at a game development company  

- Your latest mobile game has two events you're interested in tracking: `buy a
  sword` & `join guild`

- Each has metadata characterstic of such events (i.e., sword type, guild name,
  etc)


## Tasks

- Instrument your API server to log events to Kafka

- Assemble a data pipeline to catch these events: use Spark streaming to filter
  select event types from Kafka, land them into HDFS/parquet to make them
  available for analysis using Presto. 

- Use Apache Bench to generate test data for your pipeline.

- Produce an analytics report where you provide a description of your pipeline
  and some basic analysis of the events. Explaining the pipeline is key for this project!

- Submit your work as a git PR as usual. AFTER you have received feedback you have to merge 
  the branch yourself and answer to the feedback in a comment. Your grade will not be 
  complete unless this is done!

Use a notebook to present your queries and findings. Remember that this
notebook should be appropriate for presentation to someone else in your
business who needs to act on your recommendations. 

It's understood that events in this pipeline are _generated_ events which make
them hard to connect to _actual_ business decisions.  However, we'd like
students to demonstrate an ability to plumb this pipeline end-to-end, which
includes initially generating test data as well as submitting a notebook-based
report of at least simple event analytics. That said the analytics will only be a small
part of the notebook. The whole report is the presentation and explanation of your pipeline 
plus the analysis!


## Files of Interest:

- [`proj3_writeup.md`](proj3_writeup.md): My main report for the project.
- [`game_api.py`](game_api.py): our flask app for our game api.
- [`docker-compose.yml`](docker-compose.yml)
- [`anand-patel-history.txt`](anand-patel-history.txt): my history file.
- [`ab2.sh`](ab2.sh): bash script sending apache workbench HTTP commands to flask app for streaming mode.
- [`stream_and_hive.py`](stream_and_hive.py): Spark script to write data to parquet tables in streaming mode.


### Misc Files

- [`ab.sh`](ab.sh): sending apache workbench HTTP commands to flask app for batch mode.
- [`write_hive_table.py`](write_hive_table.py): Spark script to write data to parquet tables in batch mode.

