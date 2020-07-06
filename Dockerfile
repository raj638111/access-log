# Build this image with docker build -f f1 or docker build
#ARG ubuntu_version=18.04
#FROM ubuntu:${ubuntu_version}
#Use ubuntu 18:04 as your base image
FROM ubuntu:18.04
#Any label to recognise this image.
LABEL image=Spark-base-image
#Run the following commands on my Linux machine
#install the below packages on the ubuntu image
RUN apt-get update -qq && \
    apt-get install -qq -y gnupg2 wget openjdk-8-jdk scala
#Download the Spark binaries from the repo
WORKDIR /
RUN wget --no-verbose https://downloads.apache.org/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz && \
	  wget --no-verbose https://dl.bintray.com/sbt/debian/sbt-1.3.4.deb
# Untar the downloaded binaries , move them the folder name spark and add the spark bin on my class path
RUN tar -xzf spark-3.0.0-bin-hadoop3.2.tgz && \
    mv spark-3.0.0-bin-hadoop3.2 spark && \
    echo "export PATH=$PATH:/spark/bin" >> ~/.bashrc
RUN dpkg -i sbt-1.3.4.deb
#Expose the UI Port 4040
EXPOSE 4040
