Table of Contents
=================

   * [What does this Spark App do?](#what-does-this-spark-app-do)
   * [How to run the Dockerized App and validate results?](#how-to-run-the-dockerized-app-and-validate-results)
        * [Download the image from Docker Hub...](#download-the-image-from-docker-hub)
        * [Start a container with the downloaded image...](#start-a-container-with-the-downloaded-image)
        * [Bash to the container &amp; submit the spark job...](#bash-to-the-container--submit-the-spark-job)
        * [Validate the output (Using spark-shell)...](#validate-the-output-using-spark-shell)
        * [Check malformed input records](#check-malformed-input-records)
   * [Code Structure](#code-structure)
   * [Source code, ... and How to compile?](#source-code--and-how-to-compile)
        * [Prerequisites](#prerequisites)
        * [Clone the project...](#clone-the-project)
        * [Build the project...](#build-the-project)
   * [Unit tests, Code Coverage...](#unit-tests-code-coverage)
   * [Cleaning up Docker image &amp; container](#cleaning-up-docker-image--container)
        * [Cleanup Container](#cleanup-container)
        * [Cleanup Image](#cleanup-image)


# What does this Spark App do?

- This spark application computes `topN` **visitors** & **URL** for the dataset `ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz`
- For the demo, the app is dockerized and available in Docker Hub

# How to run the Dockerized App and validate results?

:point_right: **Note**: The dockerized app is only for demo and uses `--master local[*]`. For prod deployment, we would still need to deploy the standalone jar. Although it is possible to configure the Dockerized app to connect to specific Spark Master (or) Mesos (or) Yarn, this dockerized app is not configured / tested for that.

### Download the image from Docker Hub...

```
// Download the image
docker pull rjkumratgmaildotcom/topn:latest

// Verify if the image is downloaded

docker images

REPOSITORY                 TAG                 IMAGE ID            CREATED             SIZE
rjkumratgmaildotcom/topn   latest              db07e6c99103        About an hour ago   1GB


```

### Start a container with the downloaded image...

```
// Start the container

docker run --rm  -dit  --name c1 rjkumratgmaildotcom/topn:latest

where,
  c1    = The container name
  --rm  = Ensures we can remove the container using `stop` 
          command without the need to explicitly provide `rm` command
          after `stop`

// Ensure the container is running

docker ps
CONTAINER ID        IMAGE                      COMMAND             CREATED             STATUS              PORTS               NAMES
24a22b6e08df        rjkumratgmaildotcom/topn   "/bin/bash"         14 minutes ago      Up 14 minutes                           c1        
```

### Bash to the container & submit the spark job...

```
// Open the Bash shell in Container
docker exec -it c1 /bin/bash

// you are in this location
pwd
/

ls 
access-log-analytics-assembly-0.1.0-SNAPSHOT.jar  ... spark-3.0.0-bin-hadoop3.2.tgz  ...

// Submit spark job 

Note: The job would take ~1 to 2 minutes to complete

spark-submit --master local[*] \
--driver-memory 3G \
--conf spark.hadoop.fs.ftp.data.connection.mode="PASSIVE_LOCAL_DATA_CONNECTION_MODE" \
--class com.secureworks.analytics.accesslog.TopVisitorsNUrl \
./access-log-analytics-assembly-0.1.0-SNAPSHOT.jar \
--inputPath ftp://anonymous:pwd@ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz \
--partitionCount 8 \
--topN 3 \
--dbNtable demo.test \
--outputPath file:///target/demo/test \
--invalidTolerance 10 \
--invalidDataTbl "demo.invalid_data"

where, 
    inputPath        = Is the actual FTP input
    partitionCount   = Is the repartition count
    topN             = Is the topN limit
                       Ex: 3: Will get top 3 URL & Visitors for each date
    dbNtable         = The hive external database.table where we 
                       store the result
    outputPath       = The path linked to hive external table 
    invalidTolerance = No of acceptable malformed records in input data.
                       If exceeds, application throws exception to 
                       alert the user. The result will still be 
                       stored to the table <dbNtable>
    invalidDataTbl   = All malformed records are stored in this
                       managed table 
```

### Validate the output (Using spark-shell)...

:point_right: **Note**: Ensure not to change directory after spark-submit in the previous step. As the docker container is not configured with any proper database (MySQL etc...) as metastore, the local derby derby database is created in the location where spark-submit is ran at the previous step

```
pwd
/

ls 
access-log-analytics-assembly-0.1.0-SNAPSHOT.jar  derby.log  ...  
 spark-3.0.0-bin-hadoop3.2.tgz  ... metastore_db ... 

spark-shell
scala> spark.table("demo.test").where("dt='1995-07-01'").orderBy("dt").show(100, false)
+-----+----------------------------+---+----------+--------+
|count|value                       |rnk|dt        |sort_col|
+-----+----------------------------+---+----------+--------+
|619  |piweba3y.prodigy.com        |1  |1995-07-01|visitor |
|545  |piweba4y.prodigy.com        |2  |1995-07-01|visitor |
|533  |alyssa.prodigy.com          |3  |1995-07-01|visitor |
|3960 |/images/NASA-logosmall.gif  |1  |1995-07-01|url     |
|3521 |/images/KSC-logosmall.gif   |2  |1995-07-01|url     |
|2684 |/shuttle/countdown/count.gif|3  |1995-07-01|url     |
+-----+----------------------------+---+----------+--------+
where,
    count    = No of hits
    rnk      = Specifies the top N rank (we are using dense_rank()
                                    to compute topN)
    sort_col = The criteria for which the topN is calculated
               Right now this has 2 values (`visitor` or `url`) as 
               per the project requirement
               
    value    = Value of the sort_col   
```

### Check malformed input records

Malformed input records are stored in table `demo.invalid_data` for later reference.  

:exclamation: Eight Malformed records are found in ftp://anonymous:pwd@ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
```
spark-shell
scala> spark.table("demo.invalid_data").limit(100).show(100, false)
+-------------------------------------------------------------------------------------------------------+
|str                                                                                                    |
+-------------------------------------------------------------------------------------------------------+
|klothos.crl.research.digital.com - - [10/Jul/1995:16:45:50 -0400] "" 400 -                             |
|jumbo.jet.uk - - [11/Jul/1995:08:06:29 -0400] "GET  HTTP/1.0" 302 -                                    |
 ...
|alyssa.p                                                                                               |
+-------------------------------------------------------------------------------------------------------+
```

# Code Structure

```
├── Dockerfile  // Contains config to package the App with Docker
├── README.md
├── build.sbt
├── derby.log
├── metastore_db // Derby is used as hive metastore in Local environment
│   ...
├── project
│   ├── build.properties
│   ├── plugins.sbt
├── spark-warehouse
│   └── demo.db 
├── src
│   ├── main
│   │   └── scala
│   │       └── com
│   │           └── secureworks
│   │               └── analytics
│   │                   ├── accesslog
│   │                   │   ├── AccessInfo.scala       // Data object used to store parsed info
│   │                   │   ├── Param.scala            // Parser for command line arguments
│   │                   │   └── TopVisitorsNUrl.scala  // Our spark application
│   │                   └── utils
│   │                       └── Log.scala              // Custom logger to avoid log noise
│   └── test
│       └── scala
│           └── com
│               └── secureworks
│                   └── analytics
│                       └── accesslog
│                           └── TopVisitorsNUrlTest.scala // Unit tests
```

# Source code, ... and How to compile?

### Prerequisites
Ensure that `sbt` (version 1.3.4) is installed in Local machine

### Clone the project...

```
git clone https://github.com/raj638111/access-log-analytics.git

cd access-log-analytics
```

### Build the project...

```
sbt assembly
```

^ This creates a Fat jar `target/scala-2.12/access-log-analytics-assembly-0.1.0-SNAPSHOT.jar`

# Unit tests, Code Coverage...

The unit tests for the application is written using [ScalaTest](https://www.scalatest.org/)

All the unit tests can be run using `sbt test`
```
sbt test

[info] TopVisitorsNUrlTest:
[info] - String to java.sql.Date conversion
[info] - Parse Single Line: Valid format
...
[info] All tests passed
```

Code coverage can be checked using the `jacoco` plugin as `sbt jacoco`
```
sbt jacoco

[info] ------- Jacoco Coverage Report -------
[info]
[info] Lines: 70% (>= required 0.0%) covered, 33 of 110 missed, OK
...
```


# Cleaning up Docker image & container

Given below are the commands if needed to cleanup the 
container and image

### Cleanup Container

```
-- List running / non-running container
docker ps -a

-- Stop container
docker container stop <container name / ID>

-- Remove container
docker container stop <container name / ID>

```

### Cleanup Image

```
-- List all images

docker images

-- Delete image

docker image rm <image name / ID>       

```