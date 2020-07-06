
# What does this Spark App do?

This spark application computes `topN` **visitors** & **URL** for the dataset available in FTP server ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz

# How to setup build environment?

Build environment to compile & test the code can be created in the Local development machine using [Dockerfile](Dockerfile), which creates the Docker image with required prerequisites like `Spark` & `SBT` 

- Ensure docker is installed and running in Local machine
- From the `${PROJECT_HOME}` directory, create docker image
 ```
// Create docker image
docker build .

// Check if docker image is available
docker images
REPOSITORY          TAG                 IMAGE ID            CREATED             SIZE
<none>              <none>              884114325a95        5 hours ago         999MB
ubuntu              18.04               8e4ce0a6ce69        2 weeks ago         64.2MB

// Note down the docker IMAGE ID
In this example, IMAGE ID is 884114325a95
 ```   
- Create and run a container using Docker image
```
-- Create the Container named mySpark

docker run --rm \
-v <PROJECT_HOME>:/access-log-analytics \
-dit --name mySpark 884114325a95
                        ^ This is the Docker image ID we got from 
                          previous step
             ^ mySpark is the name we give to the container
where,            
    PROJECT_HOME Is the absolute path of the directory in which you have
                 clone the GIT repo
                 Example: /home/someuser/workspace/access-log-analytics

The above command creates a container called mySpark 
and also maps the Host machine directory,
    <PROJECT_HOME>:/access-log-analytics 
where you have cloned to the GIT repo to the container 
volume `/access-log-analytics`            
```

- Access the container through shell prompt

```
-- Access the container
docker exec -it mySpark /bin/bash

-- Check if project directory is accessible in the container
ls
access-log-analytics  boot  etc   lib  ... spark ...
    ^ 
    Project directory should be available    
```

- Cleaning up Docker image & container
```
-- Stop & Remove docker container
docker container stop mySpark

-- Delete image
docker image rm <image name / ID>       
```

# How to compile & create Fat Jar?
```
// Move to project home 
cd /access-log-analytics 
(If in container)
    (or)
cd <PROJECT_HOME>
(If in Host machine) 

// Run all unit tests & build fat jar
sbt assembly
    ^ The first time this command is run in the container
      it will take some time to complete, as many dependencies
      would need to be downloaded
[info] [launcher] getting org.scala-sbt sbt 1.3.4  (this may take some time)...
...
[info] TopVisitorsNUrlTest:
[info] - String to java.sql.Date conversion
[info] - Parse Single Line: Valid format
...
[info] All tests passed
```

# How to run unit tests & code coverage?

The unit test for the application is written using [ScalaTest](https://www.scalatest.org/)

All the unit test can be run using `sbt test`
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

# How to run the spark application?

From the `${PROJECT_HOME}`,
```
spark-submit --master local[*] \
--executor-memory 2G --driver-memory 2G \
--class com.secureworks.analytics.accesslog.TopVisitorsNUrl \
./target/scala-2.12/access-log-analytics-assembly-0.1.0-SNAPSHOT.jar \
--inputPath ftp://anonymous:anonpwd@ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz \
--partitionCount 8 \
--topN 3 \
--dbNtable demo.test \
--outputPath file:///Users/raj/ws/access-log-analytics/target/demo/test

where,
--inputPath      Location of the input data
--partitionCount Repartition count to parallelize computation
--topN           The top N limit
--dbNtable       Hive external table(db.table) in which result 
                 will be stored
--outputPath     Hive external table path

NOTE: If in prod, replace `--master local[*]` with the required cluster
manager, which could be `yarn` (or) `mesos` (or) `standalone`
```

As we are using only the Local derby database as a metastore for Hive, 
we should be able to see the derby metastore created in the current directory
```
ls
Dockerfile	build.sbt	metastore_db**	spark-warehouse**	target
README.md	derby.log**	project		src
```


# Validating the output 

**LIMITATIONS**: The development environment use the Local Derby database as metastore and is NOT configured with MySQL or PostgreSQl as a metastore (Because of the limitation in time)
So the validation needs to be performed from the same directory
in which we ran the application in previous step
Also, we do not have a separate Hive installation, so we will be
using `spark-shell` to validate the output

```
spark-shell
scala> spark.table("demo.test").show(100, false)
+-----+---------------------------------------+---+----------+--------+
|count|value                                  |rnk|dt        |sort_col|
+-----+---------------------------------------+---+----------+--------+
|545  |piweba4y.prodigy.com                   |2  |1995-07-01|visitor |
|533  |alyssa.prodigy.com                     |3  |1995-07-01|visitor |
|619  |piweba3y.prodigy.com                   |1  |1995-07-01|visitor |
|3960 |/images/NASA-logosmall.gif             |1  |1995-07-01|url     |
|2684 |/shuttle/countdown/count.gif           |3  |1995-07-01|url     |
|3521 |/images/KSC-logosmall.gif              |2  |1995-07-01|url     |
...
where,
    count    = No of hits
    rnk      = Specifies the top N rank (we are using dense_rank()
                                    to compute topN)
    sort_col = The criteria for which the topN is calculated
               Right now this has 2 values as per the project
               requirement
               `visitor` or `url`
    value    = Value of the sort_col   

```
 

