# Hops-Util
Hops-Util is a helper library for Hops. It greatly facilitates the Hops developer by hiding the complexity of discovering services and setting up security for programs interacting with Hops services. 

## Build with Maven
```
mvn clean package 
```
This generates under target/hops-util-0.1.jar a fat-jar that includes all the required dependencies.

## API
Hops-Util provides an API that automatically sets up Apache Kafka producers and consumers for both Apache Spark and Apache Flink as well as providing methods for discovering endpoints of various Hops services such as InfluxDB.

## Usage
The latest version of Hops-Util is available with the Hops distribution. When creating and submitting a job in HopsWorks, Hops-Util is automatically distributed on all the nodes managed by YARN on which the job will run. 

If you want to make changes or append functionality to the library, the new version can be used with the submitted job by providing Hops-Util as a library when creating the job via the job service in HopsWorks. This will override the default Hops-Util available in the platform. 

To use Hops-Util, you should include the following dependency your application's POM file. 
```
<dependency>
  <groupId>io.hops</groupId>
  <artifactId>hops-util</artifactId>
  <version>0.1</version>
</dependency>
```

and the following repository under your repositories list,
```
<repository>
  <id>sics-release</id>
  <name>SICS Release Repository</name>
  <url>http://kompics.sics.se/maven/repository</url>
  <releases>
  </releases>
  <snapshots>
    <enabled>false</enabled>
  </snapshots>
</repository>
```

## Example
To create a Kafka Spark Streaming consumer using Hops-Util is as simple as this,
```
JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
SparkConsumer consumer = HopsUtil.getSparkConsumer(jssc);
```

Management of topics and consumer groups as well as distribution of SSL/TLS certificates is automatically performed by the utility. The developer needs only to care about implementing the application's business logic. A complete example on how to use Hops-Util for implementing a Kafka Spark-Streaming app is available [here](https://github.com/hopshadoop/hops-kafka-examples/blob/master/spark/src/main/java/io/hops/examples/spark/kafka/StreamingExample.java).
