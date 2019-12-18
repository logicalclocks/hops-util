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
HopsUtil API provides default security configurations for Kafka clients in Spark and utility methods to retrieve 
endpoints of various Hopsworks services. Moreover the API provides utility methods for interacting with the Hopsworks
 Feature Store.
**Javadoc for HopsUtil is available** [here](http://snurran.sics.se/hops/hops-util-javadoc).


### Feature Store API

The HopsUtil API makes it easy to write/read to Hopsworks Feature Store.

#### Read a list of features from the Feature Store

``` scala
import io.hops.util.Hops
import scala.collection.JavaConversions._
import collection.JavaConverters._

val features = List("team_budget", "average_attendance", "average_player_age")
val featuresDf = Hops.getFeatures(features).read()
```

#### SQL Query to the Feature Store

``` scala
import io.hops.util.Hops
Hops.queryFeaturestore(
    "SELECT team_budget, score " +
    "FROM teams_features_1 JOIN games_features_1 ON " +
    "games_features_1.home_team_id = teams_features_1.team_id").read().show(5)
```

#### Write to the Feature Store

``` scala
import io.hops.util.Hops

Hops.createFeaturegroup("teams_features_spanish").setDataframe(teamsFeaturesDf2).write()      
```

A complete example of the Scala/Java API for the Feature Store is available [here](https://github.com/logicalclocks/hops-examples).
