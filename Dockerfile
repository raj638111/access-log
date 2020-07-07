#Use ubuntu 18:04 as your base image
FROM ubuntu:18.04

#Any label to recognise this image.
LABEL image=Spark-base-image

#Run the following commands on my Linux machine
#install the below packages on the ubuntu image
RUN apt-get update -qq && \
    apt-get install -qq -y gnupg2 wget openjdk-8-jdk scala

WORKDIR /

COPY ./target/scala-2.12/access-log-analytics-assembly-0.1.0-SNAPSHOT.jar .

#Download the Spark binaries from the repo
RUN wget --no-verbose https://downloads.apache.org/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz 

# Untar the downloaded binaries , move them the folder name spark and add the spark bin on my class path
RUN tar -xzf spark-3.0.0-bin-hadoop3.2.tgz && \
    mv spark-3.0.0-bin-hadoop3.2 spark && \
    echo "export PATH=$PATH:/spark/bin" >> ~/.bashrc


