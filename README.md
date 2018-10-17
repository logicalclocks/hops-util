# hops-util
**hops-util** is a library facilitating development of Java/Scala programs for Hopsworks. It assists the developer by
 hiding the complexity of having to discover services and setting up security for programs interacting with 
 Hopsworks services. Such services include the Hopsworks REST API, Apache Kafka, Apache Spark etc. For detailed 
 documentation see [here](https://github.com/logicalclocks/hopsworks/).

hops-util is automatically deployed when users run jobs/notebooks in Hopsworks. If users need to make 
changes to the library itself, they can build it and provide it as an additional resource to their job/notebook (see
 [doc](https://hops.readthedocs.io/en/latest/user_guide/hopsworks/jupyter.html)).
 
## Build
To build hops-util you need to have maven installed. Then simply do,

```
mvn clean package 
```
which generates under the `target` directory two archives, a thin jar that is deployed on Hops maven repository and a
fat jar containing all the required dependencies to be used from within Hopsworks .

## Usage
The latest version of hops-util is available in Hopsworks. When creating and submitting a job in 
Hopsworks, hops-util is automatically distributed on all the nodes managed by YARN on which the job will run. 

If you want to make changes or append functionality to the library, the new version can be used with the submitted 
job  by providing hops-util as a library when creating the job via the job service in HopsWorks. This will override 
the  default hops-util available in the platform. 

To include hops-util in your maven project, you should include the following dependency your application's POM file. 
```
<dependency>
  <groupId>io.hops</groupId>
  <artifactId>hops-util</artifactId>
  <version>0.6.0-SNAPSHOT</version>
</dependency>
```

and the following repository under your repositories list,
```
<repository>
  <id>Hops</id>
  <name>Hops Repo</name>
  <url>https://bbc1.sics.se/archiva/repository/Hops/</url>
  <releases>
    <enabled>true</enabled>
  </releases>
  <snapshots>
    <enabled>true</enabled>
  </snapshots>
</repository>
```

## API
hops-util provides an API that automatically sets up Apache Kafka producers and consumers for both Apache Spark and 
Apache Flink as well as providing methods for discovering endpoints of various Hopsworks services such as InfluxDB.

**Javadoc for hops-util is available** [here](http://snurran.sics.se/hops/hops-util-javadoc/0.6.0-SNAPSHOT).

### Job Workflows
It is also possible to build simple Hopsworks job workflows using hops-util. The two methods provided are:
* **startJobs**: Gets a number of job IDs as input parameter and starts the respective jobs of the project for which 
the user invoking the jobs is also their creator. It can be used like `Hops.startJobs(1);`
* **waitJobs**: Waits for jobs (supplied as comma-separated job IDs) to transition to a running (default) state or 
not_running, depending whether an optional boolean parameter is true or not. It can be used like `waitJobs(1,5,11);`,
which means the method will return when all three jobs with IDs 1,5,11 are not running, or `waitJobs(false, 1,5,11);`
 which means the method will return when all jobs have entered the running state.

The ID of a job is displayed in the Hopsworks Job Details page, as shown below.
![Job ID](./src/main/resources/job_id.png)

## Example
To create a Kafka Spark StructuredStreaming consumer using hops-util is as simple as this,
```
DataStreamReader dsr = Hops.getSparkConsumer().getKafkaDataStreamReader();
```

and to gracefully shut it down you can do
```
Hops.shutdownGracefully(queryFile);
```
where queryFile is the Spark *StreamingQuery* object.

Management of topics and consumer groups as well as distribution of SSL/TLS certificates is automatically performed 
by the utility. The developer needs only to care about implementing the application's business logic. A complete 
example on how to use hops-util for implementing a Kafka Spark-Streaming app is available 
[here](https://github.com/hopshadoop/hops-kafka-examples/blob/master/spark/src/main/java/io/hops/examples/spark/kafka/StructuredStreamingKafka.java).
