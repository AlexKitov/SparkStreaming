# Spark Streaming

1. Basic streaming from files:
    org.company.temperature.DStreaming contains the solution for req_1 to req_4

2. Environment
    - there is a HDFS running ( config address in .src/main/resources/application.conf ) defaults to ```hdfs://localhost:9000/```

3. Build the app
    - clone the repo ```git clone https://github.com/AlexKitov/SparkStreaming.git```
    - cd SparkStreaming
    - mvn clean
    - mvn package

4. Run the app

    - execute ```sh init.sh``` to initialise the needed folders in ```hdfs```
    - run the app:
      Option 1 :
        - submit the job
            ```spark-submit \
              --class org.company.temperature.DStreaming.RunApp \
              --master local[*] \
              target/SparkStreaming-1.0-SNAPSHOT.jar
            ```
      Option 2 :
        - start ```org.company.temperature.DStreaming.RunApp``` object from IDE
    - copy files from local resources to trigger the DStream ```sh addstreams.sh```
    - before next run of the app delete the files, so DStream can detect change on next copy ```sh deletestreams.sh```

5. Troubleshooting
   - in case data does not appear most probably there were already some files inside the input folders.
    Run ```sh deletestreams.sh```, ```restart the app``` and then copy the files again ```sh addstreams.sh``` 

6. TODO
    - add tests
    - setup Kafka connect (link???) to pre-process the *.xml files (if data comes as files and not new line delimited xml stream)
    - finish StructuredStreaming (currently stuck on limitation that Dataset can only have 1 agg, and it is used to parse the multi line xmls)
    - setup docker hadoop and connect to remote (container) HDFS (currently stuck on EOF Exception)
