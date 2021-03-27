# Spark Streaming

1. Basic streaming from files

2. Environment
- there is a HDFS running ( config address in .src/main/resources/application.conf ) defaults to ```hdfs://localhost:9000/```
- there are data files in HDFS ```temperatures``` folder ( ```hdfs dfs -ls /temperatures```)
- there is an empty folder ```test``` (```hdfs dfs -ls /test```)

3. Run the app
- copy files to see result - ```hdfs dfs -cp /temperatures/*2020080* /test```
- remove files before next run - ```hdfs dfs -rm -r /test/*```