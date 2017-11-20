# HopsUtil
HopsUtil is a helper library for Hops. It greatly facilitates the Hops developer by hiding the complexity of discovering services and setting up security for programs interacting with Hops services. 

## Build
To build HopsUtil you need to have maven installed. Then simply do

```
mvn clean package 
```
This generates under target/hops-util-0.1-0.jar a fat-jar that includes all the required dependencies.

## API
HopsUtil provides an API that automatically sets up Apache Kafka producers and consumers for both Apache Spark and Apache Flink as well as providing methods for discovering endpoints of various Hops services such as InfluxDB.

**Javadoc for HopsUtil is available** [here](http://snurran.sics.se/hops/hops-util-javadoc/0.1.0/).

### Job Workflows
It is also possible to build simple Hopsworks job workflows using HopsUtil. The two methods provided are:
* **startJobs**: Gets a number of job IDs as input parameter and starts the respective jobs of the project for which the user invoking the jobs is also their creator. It can be used like `HopsUtil.startJobs(1);`
* **waitJobs**: Gets a number of job IDs to wait as long as the jobs are either in a running (default) state or not, depending on the input parameter. It can be used like `waitJobs(1,5,11);`, which means the method will return when all three jobs are not running, or `waitJobs(false, 1,5,11);` which means the method will return when all jobs are in a running state.

The ID of a job is displayed in the Hopsworks Job Details page, as shown below.
![Job ID](./src/main/resources/job_id.png)

## Usage
The latest version of HopsUtil is available with the Hops distribution. When creating and submitting a job in HopsWorks, Hops-Util is automatically distributed on all the nodes managed by YARN on which the job will run. 

If you want to make changes or append functionality to the library, the new version can be used with the submitted job by providing HopsUtil as a library when creating the job via the job service in HopsWorks. This will override the default HopsUtil available in the platform. 

To use HopsUtil, you should include the following dependency your application's POM file. 
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
To create a Kafka Spark StructuredStreaming consumer using HopsUtil is as simple as this,
```
DataStreamReader dsr = HopsUtil.getSparkConsumer().getKafkaDataStreamReader();
```

and to gracefully shut it down you can do
```
HopsUtil.shutdownGracefully(queryFile);
```
where queryFile is the Spark *StreamingQuery* object.

Management of topics and consumer groups as well as distribution of SSL/TLS certificates is automatically performed by the utility. The developer needs only to care about implementing the application's business logic. A complete example on how to use Hops-Util for implementing a Kafka Spark-Streaming app is available [here](https://github.com/hopshadoop/hops-kafka-examples/blob/master/spark/src/main/java/io/hops/examples/spark/kafka/StructuredStreamingKafka.java).
