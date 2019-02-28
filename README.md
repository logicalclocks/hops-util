# HopsUtil
**HopsUtil** is a library facilitating development of Java/Scala programs for Hopsworks. It assists the developer by
 hiding the complexity of having to discover services and setting up security for programs interacting with
 Hopsworks services. Such services include the Hopsworks REST API, Apache Kafka, Apache Spark, Hopsworks Feature Store etc. For detailed
 documentation see [here](https://github.com/logicalclocks/hopsworks/). *For the python version of this library, see
 [here](https://github.com/logicalclocks/hops-util-py)*.

HopsUtil is automatically deployed when users run jobs/notebooks in Hopsworks. If users need to make
changes to the library itself, they can build it and provide it as an additional resource to their job/notebook (see
 [doc](https://hopsworks.readthedocs.io/en/latest/user_guide/hopsworks/jupyter.html)).

## Build
To build HopsUtil you need to have maven installed. Then simply do,

```
mvn clean package javadoc:javadoc
```
which generates under the `target` directory two archives, a thin jar that is deployed on Hops maven repository and a
fat jar containing all the required dependencies to be used from within Hopsworks .

## Usage
The latest version of HopsUtil is available in Hopsworks. When creating and submitting a job or opening a notebook server in
Hopsworks, HopsUtil is automatically distributed on all the nodes managed by YARN on which the job/notebook will run.

If you want to make changes or append functionality to the library, the new version can be used with the submitted
job  by providing HopsUtil as a library when creating the job via the job service in HopsWorks. This will override
the  default HopsUtil available in the platform.

To include HopsUtil in your maven project, you should include the following dependency your application's POM file.
```
<dependency>
  <groupId>io.hops</groupId>
  <artifactId>hops-util</artifactId>
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
HopsUtil provides an API that automatically sets up Apache Kafka producers and consumers for both Apache Spark and
Apache Flink as well as providing methods for discovering endpoints of various Hopsworks services such as InfluxDB. Moreover the API provides utility methods for interacting with the Hopsworks Feature Store

**Javadoc for HopsUtil is available** [here](http://snurran.sics.se/hops/hops-util-javadoc).

### Job Workflows
It is also possible to build simple Hopsworks job workflows using HopsUtil. The two methods provided are:
* **startJobs**: Gets a number of job IDs as input parameter and starts the respective jobs of the project for which
the user invoking the jobs is also their creator. It can be used like `Hops.startJobs(1);`
* **waitJobs**: Waits for jobs (supplied as comma-separated job IDs) to transition to a running (default) state or
not_running, depending whether an optional boolean parameter is true or not. It can be used like `waitJobs(1,5,11);`,
which means the method will return when all three jobs with IDs 1,5,11 are not running, or `waitJobs(false, 1,5,11);`
 which means the method will return when all jobs have entered the running state.

The ID of a job is displayed in the Hopsworks Job Details page, as shown below.
![Job ID](./src/main/resources/job_id.png)

## Example
To create a Kafka Spark StructuredStreaming consumer using HopsUtil is as simple as this,
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
example on how to use HopsUtil for implementing a Kafka Spark-Streaming app is available
[here](https://github.com/hopshadoop/hops-kafka-examples/blob/master/spark/src/main/java/io/hops/examples/spark/kafka/StructuredStreamingKafka.java).

### Feature Store API

The HopsUtil API makes it easy to write/read to Hopsworks Feature Store.

#### Read a list of features from the Feature Store

``` scala
import io.hops.util.Hops
import scala.collection.JavaConversions._
import collection.JavaConverters._

val features = List("team_budget", "average_attendance", "average_player_age")
Hops.getFeatures(spark, features, Hops.getProjectFeaturestore).show(5)
```

#### SQL Query to the Feature Store

``` scala
import io.hops.util.Hops
Hops.queryFeaturestorequeryFea (spark,
    "SELECT team_budget, score " +
    "FROM teams_features_1 JOIN games_features_1 ON " +
    "games_features_1.home_team_id = teams_features_1.team_id", Hops.getProjectFeaturestore).show()
```

#### Write to the Feature Store

``` scala
import io.hops.util.Hops
import scala.collection.JavaConversions._
import collection.JavaConverters._

val jobId = null
val dependencies = List[String]().asJava
val primaryKey = null
val descriptiveStats = false
val featureCorr = false
val featureHistograms = false
val clusterAnalysis = false
val statColumns = List[String]().asJava
val numBins = null
val corrMethod = null
val numClusters = null
val description = "a spanish version of teams_features"

Hops.createFeaturegroup(
    spark, teamsFeaturesDf2, "teams_features_spanish", Hops.getProjectFeaturestore,
    1, description, jobId,
    dependencies, primaryKey, descriptiveStats, featureCorr,
      featureHistograms, clusterAnalysis, statColumns, numBins,
      corrMethod, numClusters)
```

A complete example of the Scala/Java API for the Feature Store is available [here](https://github.com/Limmen/hops-examples/blob/HOPSWORKS-721/notebooks/featurestore/FeaturestoreTourScala.ipynb).
