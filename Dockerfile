# Build this image with docker build -f f1 or docker build
#ARG ubuntu_version=18.04
#FROM ubuntu:${ubuntu_version}
#Use ubuntu 18:04 as your base image
FROM ubuntu:18.04
#Any label to recognise this image.
LABEL image=Spark-base-image
#ENV SPARK_VERSION=3.0.0
#ENV HADOOP_VERSION=2.7
#Run the following commands on my Linux machine
#install the below packages on the ubuntu image
RUN apt-get update -qq && \
    apt-get install -qq -y gnupg2 wget openjdk-8-jdk scala
#Download the Spark binaries from the repo
WORKDIR /
RUN wget --no-verbose https://mirrors.ocf.berkeley.edu/apache/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz
# Untar the downloaded binaries , move them the folder name spark and add the spark bin on my class path
RUN tar -xzf /spark-3.0.0-bin-hadoop2.7.tgz && \
    mv spark-3.0.0-bin-hadoop2.7 spark && \
    echo "export PATH=$PATH:/spark/bin" >> ~/.bashrc

RUN apt-get install ftp
#Expose the UI Port 4040
EXPOSE 4040
