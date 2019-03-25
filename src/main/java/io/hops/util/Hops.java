/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hops.util;

import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.HTTPSClientInitializationException;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.SchemaNotFoundException;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadFeature;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadFeaturegroup;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadFeaturegroupLatestVersion;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadFeaturegroups;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadFeatures;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadFeaturesList;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadMetadata;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadProjectFeaturestore;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadProjectFeaturestores;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadTrainingDataset;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadTrainingDatasetLatestVersion;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadTrainingDatasetPath;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadTrainingDatasets;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreSQLQuery;
import io.hops.util.featurestore.ops.write_ops.FeaturestoreCreateFeaturegroup;
import io.hops.util.featurestore.ops.write_ops.FeaturestoreCreateTrainingDataset;
import io.hops.util.featurestore.ops.write_ops.FeaturestoreInsertIntoFeaturegroup;
import io.hops.util.featurestore.ops.write_ops.FeaturestoreInsertIntoTrainingDataset;
import io.hops.util.featurestore.ops.write_ops.FeaturestoreUpdateFeaturegroupStats;
import io.hops.util.featurestore.ops.write_ops.FeaturestoreUpdateMetadataCache;
import io.hops.util.featurestore.ops.write_ops.FeaturestoreUpdateTrainingDatasetStats;
import org.apache.avro.Schema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class to be used by applications that want to communicate with Hopsworks.
 * Users can call the getters within their Hopsworks jobs to get the provided properties.
 */
public class Hops {

  private static final Logger LOG = Logger.getLogger(Hops.class.getName());

  private static Integer projectId;
  private static String projectName;
  private static String jobName;
  private static String appId;
  private static String jobType;
  private static List<String> brokerEndpointsList;
  private static String brokerEndpoints;
  private static String restEndpoint;
  private static String keyStore;
  private static String trustStore;
  private static String keystorePwd;
  private static String truststorePwd;
  private static String elasticEndPoint;

  private static WorkflowManager workflowManager;

  static {
    setup();
  }

  private Hops() {

  }

  /**
   * Setup the static Hops instance upon instantiation.
   * <p>
   */
  private static synchronized void setup() {
    Properties sysProps = System.getProperties();
    //If the sysProps are properly set, it is a Spark job. Flink jobs must call the setup method.
    if (sysProps.containsKey(Constants.JOBTYPE_ENV_VAR) && sysProps.getProperty(Constants.JOBTYPE_ENV_VAR).
      equalsIgnoreCase("spark")) {
      restEndpoint = sysProps.getProperty(Constants.HOPSWORKS_RESTENDPOINT);
      projectName = sysProps.getProperty(Constants.PROJECTNAME_ENV_VAR);
      keyStore = Constants.K_CERTIFICATE_ENV_VAR;
      trustStore = Constants.T_CERTIFICATE_ENV_VAR;

      //Get keystore and truststore passwords from Hopsworks
      projectId = Integer.parseInt(sysProps.getProperty(Constants.PROJECTID_ENV_VAR));
      String pwd = getCertPw();
      keystorePwd = pwd;
      truststorePwd = pwd;
      jobName = sysProps.getProperty(Constants.JOBNAME_ENV_VAR);
      appId = sysProps.getProperty(Constants.APPID_ENV_VAR);
      jobType = sysProps.getProperty(Constants.JOBTYPE_ENV_VAR);

      elasticEndPoint = sysProps.getProperty(Constants.ELASTIC_ENDPOINT_ENV_VAR);
      //Spark Kafka topics
      if (sysProps.containsKey(Constants.KAFKA_BROKERADDR_ENV_VAR)) {
        parseBrokerEndpoints(sysProps.getProperty(Constants.KAFKA_BROKERADDR_ENV_VAR));
      }
      try {
        updateFeaturestoreMetadataCache().setFeaturestore(getProjectFeaturestore().read()).write();
      } catch (JAXBException e) {
        LOG.log(Level.SEVERE,
          "Could not fetch the feature store metadata for feature store: " + Hops.getProjectFeaturestore(), e);
      } catch (FeaturestoreNotFound e) {
        LOG.log(Level.INFO,
          "Could not fetch the feature store metadata for feature store: " + Hops.getProjectFeaturestore(), e);
      }
    }
  }

  /**
   * Get Avro Schemas for all Kafka topics directly using topics retrieved from Hopsworks.
   *
   * @param topics kafka topics.
   * @return Map of schemas.
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws SchemaNotFoundException      SchemaNotFoundException
   */
  public static Map<String, Schema> getSchemas(String[] topics) throws JWTNotFoundException,
    SchemaNotFoundException {
    if(topics == null) {
      throw new IllegalArgumentException("Topics were not provided.");
    }
    Map<String, Schema> schemas = new HashMap<>();
    for (String topic : topics) {
      Schema.Parser parser = new Schema.Parser();
      schemas.put(topic, parser.parse(getSchema(topic)));
    }
    return schemas;
  }

  /**
   * Get the Avro schema for a particular Kafka topic and its version.
   *
   * @param topic     Kafka topic name.
   * @return Avro schema as String object in JSON format
   * @throws SchemaNotFoundException      SchemaNotFoundException
   * @throws JWTNotFoundException JWTNotFoundException
   */
  public static String getSchema(String topic) throws
    JWTNotFoundException, SchemaNotFoundException {
    LOG.log(Level.FINE, "Getting schema for topic:{0}", new String[]{topic});

    JSONObject json = new JSONObject();
    json.append("topicName", topic);
    Response response = null;
    try {
      response = clientWrapper(json, "/project/" + projectId + "/kafka/" + topic + "/schema", HttpMethod.GET);
    } catch (HTTPSClientInitializationException e) {
      throw new SchemaNotFoundException(e.getMessage());
    }
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      throw new SchemaNotFoundException("No schema found for topic:" + topic);
    }
    final String responseEntity = response.readEntity(String.class);
    //Extract fields from json
    LOG.log(Level.FINE, "responseEntity:" + responseEntity);
    json = new JSONObject(responseEntity);
    return json.getString("contents");
  }

  public static Properties getKafkaSSLProperties() {
    Properties properties = new Properties();
    properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    properties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, Hops.getTrustStore());
    properties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, Hops.getTruststorePwd());
    properties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, Hops.getKeyStore());
    properties.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, Hops.getKeystorePwd());
    properties.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, Hops.getKeystorePwd());
    return properties;
  }

  protected static Response clientWrapper(String path, String httpMethod) throws HTTPSClientInitializationException,
    JWTNotFoundException {
    return clientWrapper(null, path, httpMethod);
  }
  protected static Response clientWrapper(JSONObject json, String path, String httpMethod) throws
    HTTPSClientInitializationException, JWTNotFoundException {

    Client client;
    try {
      client = initClient();
    } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
      throw new HTTPSClientInitializationException("Could not retrieve credentials from local working directory", e);
    }
    WebTarget webTarget = client.target(Hops.getRestEndpoint() + "/").path(Constants.HOPSWORKS_REST_RESOURCE + path);
    LOG.log(Level.FINE, "webTarget.getUri().getHost():" + webTarget.getUri().getHost());
    LOG.log(Level.FINE, "webTarget.getUri().getPort():" + webTarget.getUri().getPort());
    LOG.log(Level.FINE, "webTarget.getUri().getPath():" + webTarget.getUri().getPath());
    //Read jwt and set it in header
    Invocation.Builder invocationBuilder =
      webTarget.request().header(HttpHeaders.AUTHORIZATION,
        "Bearer " + getJwt().orElseThrow(IllegalArgumentException::new)).accept(MediaType.APPLICATION_JSON);

    switch (httpMethod) {
      case HttpMethod.PUT:
        if (json == null) {
          //put request with empty body
          return invocationBuilder.put(Entity.json(""));
        }
        return invocationBuilder.put(Entity.entity(json.toString(), MediaType.APPLICATION_JSON));
      case HttpMethod.POST:
        if (json == null) {
          //post request with empty body
          return invocationBuilder.post(Entity.json(""));
        }
        return invocationBuilder.post(Entity.entity(json.toString(), MediaType.APPLICATION_JSON));
      case HttpMethod.GET:
        return invocationBuilder.get();
      default:
        break;
    }
    return null;
  }


  /**
   * Get keystore password from local container.
   *
   * @return Certificate password.
   */
  private static String getCertPw() {
    try (FileInputStream fis = new FileInputStream(Constants.CRYPTO_MATERIAL_PASSWORD)) {
      StringBuilder sb = new StringBuilder();
      int content;
      while ((content = fis.read()) != -1) {
        sb.append((char) content);
      }
      return sb.toString();
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, null, ex);
    }
    return null;
  }

  private static synchronized Optional<String> getJwt() throws JWTNotFoundException {
    String jwt = null;
    try (FileChannel fc = FileChannel.open(Paths.get(Constants.JWT_FILENAME), StandardOpenOption.READ)) {
      FileLock fileLock = fc.tryLock(0, Long.MAX_VALUE, true);
      try {
        short numRetries = 5;
        short retries = 0;
        while (fileLock == null && retries < numRetries) {
          LOG.log(Level.FINEST, "Waiting for lock on jwt file at:" + Constants.JWT_FILENAME);
          Thread.sleep(1000);
          fileLock = fc.tryLock(0, Long.MAX_VALUE, true);
          retries++;
        }
        //If could not acquire lock in reasonable time, throw exception
        if (fileLock == null) {
          throw new JWTNotFoundException("Could not read jwt token from local container, possibly another process has" +
            " acquired the lock");
        }
        ByteBuffer buf = ByteBuffer.allocateDirect(512);
        fc.read(buf);
        buf.flip();
        jwt = StandardCharsets.UTF_8.decode(buf).toString();
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "JWT waiting thread was interrupted.", e);
      } finally {
        if (fileLock != null) {
          fileLock.release();
        }
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE,"Could not read jwt token from local container.", e);
      throw new JWTNotFoundException("Could not read jwt token from local container." + e.getMessage(), e);
    }
    return Optional.ofNullable(jwt);
  }

  /////////////////////////////////////////////

  /**
   * Get Kafka brokers endpoints as a List object.
   *
   * @return broker endpoints.
   */
  public static List<String> getBrokerEndpointsList() {
    return brokerEndpointsList;
  }

  /**
   * Get Kafka brokers endpoints as a String object.
   *
   * @return broker endpoints.
   */
  public static String getBrokerEndpoints() {
    return brokerEndpoints;
  }

  /**
   * Get Project ID of current job.
   *
   * @return HopsWorks project ID.
   */
  public static Integer getProjectId() {
    return projectId;
  }

  /**
   * Get REST Endpoint of Hopsworks.
   *
   * @return REST endpoint.
   */
  public static String getRestEndpoint() {
    return restEndpoint;
  }

  /**
   * Get keystore location.
   *
   * @return keystore location.
   */
  public static String getKeyStore() {
    return keyStore;
  }

  /**
   * Get truststore. location.
   *
   * @return truststore location
   */
  public static String getTrustStore() {
    return trustStore;
  }

  /**
   * Get keystore password.
   *
   * @return keystore password
   */
  public static String getKeystorePwd() {
    return keystorePwd;
  }

  /**
   * Get truststore password.
   *
   * @return truststore password.
   */
  public static String getTruststorePwd() {
    return truststorePwd;
  }

  /**
   * Get HopsWorks project name.
   *
   * @return project name.
   */
  public static String getProjectName() {
    return projectName;
  }

  /**
   * Get HopsWorks elasticsearch endpoint.
   *
   * @return elasticsearch endpoint.
   */
  public static String getElasticEndPoint() {
    return elasticEndPoint;
  }

  /**
   * Get HopsWorks job name.
   *
   * @return job name.
   */
  public static String getJobName() {
    return jobName;
  }

  /**
   * Get YARN applicationId for this job.
   *
   * @return applicationId.
   */
  public static String getAppId() {
    return appId;
  }

  /**
   * Get JobType.
   *
   * @return JobType.
   */
  public static String getJobType() {
    return jobType;
  }

  /**
   * Get WorkflowManager.
   *
   * @return WorkflowManager.
   */
  public static WorkflowManager getWorkflowManager() {
    return workflowManager;
  }

  /**
   * Populates the Kafka broker endpoints List with the value of the java system property passed by Hopsworks.
   *
   * @param addresses addresses
   */
  private static void parseBrokerEndpoints(String addresses) {
    brokerEndpoints = addresses;
    brokerEndpointsList = Arrays.asList(addresses.split(","));
  }

  private static Client initClient() throws KeyStoreException, IOException, NoSuchAlgorithmException,
      CertificateException {
    KeyStore truststore = KeyStore.getInstance(KeyStore.getDefaultType());

    try (FileInputStream trustStoreIS = new FileInputStream(Constants.DOMAIN_CA_TRUSTSTORE)) {
      truststore.load(trustStoreIS, null);
    }
    return ClientBuilder.newBuilder().trustStore(truststore).
        hostnameVerifier(InsecureHostnameVerifier.INSTANCE).build();
  }


  /**
   * Gets the project's featurestore name (project_featurestore)
   *
   * @return a java object with parameters for the project's featurestore. The operation
   * can be started with read() on the object and parameters can be updated with setters
   */
  public static FeaturestoreReadProjectFeaturestore getProjectFeaturestore() {
    return new FeaturestoreReadProjectFeaturestore();
  }

  /**
   * Utility method for parsing the JSON response thrown by Hopsworks in case of errors
   *
   * @param response the JSON response to parse
   * @return a DTO with the parsed result
   */
  static HopsworksErrorResponseDTO parseHopsworksErrorResponse(Response response) {
    String jsonStrResponse = response.readEntity(String.class);
    JSONObject jsonObjResponse = new JSONObject(jsonStrResponse);
    int errorCode = -1;
    String errorMsg = "";
    String userMsg = "";
    if (jsonObjResponse.has(Constants.JSON_ERROR_CODE))
      errorCode = jsonObjResponse.getInt(Constants.JSON_ERROR_CODE);
    if (jsonObjResponse.has(Constants.JSON_ERROR_MSG))
      errorMsg = jsonObjResponse.getString(Constants.JSON_ERROR_MSG);
    if (jsonObjResponse.has(Constants.JSON_USR_MSG))
      userMsg = jsonObjResponse.getString(Constants.JSON_USR_MSG);
    return new HopsworksErrorResponseDTO(errorCode, errorMsg, userMsg);
  }

  /**
   * Gets a list of featurestores accessible in the project (i.e the project's own featurestore
   * and the featurestores shared with the project)
   *
   * @return a java object with parameters for getting a list of featurestores accessible in the project. The operation
   * can be started with read() on the object and parameters can be updated with setters
   */
  private static FeaturestoreReadProjectFeaturestores getFeaturestoresForProject(){
    return new FeaturestoreReadProjectFeaturestores();
  }

  /**
   * Inserts a spark dataframe into a featuregroup
   *
   * @param featuregroup  the name of the featuregroup to insert into
   * @return a java object with parameters for inserting into a featuregroup in the featurestore. The operation
   * can be started with write() on the object and parameters can be updated with setters
   */
  public static FeaturestoreInsertIntoFeaturegroup insertIntoFeaturegroup(String featuregroup) {
    return new FeaturestoreInsertIntoFeaturegroup(featuregroup);
  }

  /**
   * Inserts a spark dataframe into an existing training dataset
   *
   * @param trainingDataset        the name of the training dataset to insert into
   * @return a java object with parameters for inserting into a training dataset in the featurestore. The operation
   * can be started with write() on the object and parameters can be updated with setters
   *
   */
  public static FeaturestoreInsertIntoTrainingDataset insertIntoTrainingDataset(String trainingDataset) {
    return new FeaturestoreInsertIntoTrainingDataset(trainingDataset);
  }

  /**
   * Gets a featuregroup from a particular featurestore
   *
   * @param featuregroup        the featuregroup to get
   * @return a java object with parameters for reading a single featuregroup from the featurestore. The operation
   * can be started with read() on the object and parameters can be updated with setters
   */
  public static FeaturestoreReadFeaturegroup getFeaturegroup(String featuregroup) {
    return new FeaturestoreReadFeaturegroup(featuregroup);
  }

  /**
   * Gets a training dataset from a featurestore
   *
   * @param trainingDataset        the training dataset to get
   * @return a java object with parameters for reading a training dataset from the featurestore. The operation
   * can be started with read() on the object and parameters can be updated with setters
   */
  public static FeaturestoreReadTrainingDataset getTrainingDataset(String trainingDataset) {
    return new FeaturestoreReadTrainingDataset(trainingDataset);
  }

  /**
   * Gets a feature from a featurestore and a specific featuregroup.
   *
   * @param feature             the feature to get
   * @return a java object with parameters for reading a single feature from the featurestore. The operation
   * can be started with read() on the object and parameters can be updated with setters
   */
  public static FeaturestoreReadFeature getFeature(String feature) {
    return new FeaturestoreReadFeature(feature);
  }

  /**
   * Method for updating the statistics of a featuregroup (recomputing the statistics)
   *
   * @param featuregroup        the name of the featuregroup to update statistics for
   * @return a java object with parameters for updating the stats of a featuregroup. The operation
   *  can be started with write() on the object and parameters can be updated with setters
   *
   */
  public static FeaturestoreUpdateFeaturegroupStats updateFeaturegroupStats(String featuregroup) {
    return new FeaturestoreUpdateFeaturegroupStats(featuregroup);
  }

  /**
   * Method for updating the statistics of a training dataset (recomputing the statistics)
   *
   * @param trainingDataset        the name of the training dataset to update statistics for
   * @return a java object with parameters for updating the stats of a training dataset. The operation
   * can be started with write() on the object and parameters can be updated with setters.
   */
  public static FeaturestoreUpdateTrainingDatasetStats updateTrainingDatasetStats(String trainingDataset){
    return new FeaturestoreUpdateTrainingDatasetStats(trainingDataset);
  }

  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe.
   *
   * @param features                 the list of features to get
   * @return a java object with parameters for reading a list of features from the featurestore. The operation
   * can be started with read() on the object and parameters can be updated with setters.
   */
  public static FeaturestoreReadFeatures getFeatures(List<String> features) {
    return new FeaturestoreReadFeatures(features);
  }

  /**
   * Runs an SQL query on the project's featurestore
   *
   * @param query        the query to run
   * @return a java object with parameters for querying the featurestore with SQL. The operation
   * can be started with read() on the object and parameters can be updated with setters.
   */
  public static FeaturestoreSQLQuery queryFeaturestore(String query) {
    return new FeaturestoreSQLQuery(query);
  }

  /**
   * Gets a list of featurestores accessible by the current project
   *
   * @return a list of names of the featurestores
   */
  public static FeaturestoreReadProjectFeaturestores getProjectFeaturestores(){
    return getFeaturestoresForProject();
  }

  /**
   * Gets a list of all featuregroups in a featurestore
   *
   * @return a java object with parameters for reading the list of featuresgroups in the featurestore. The operation
   * can be started with read() on the object and parameters can be updated with setters.
   */
  public static FeaturestoreReadFeaturegroups getFeaturegroups() {
    return new FeaturestoreReadFeaturegroups();
  }

  /**
   * Gets a list of all feature names in a featurestore
   *
   * @return a java object with parameters for getting a list of feature names. The operation can be started with
   * read() on the object and parameters can be updated with setters
   */
  public static FeaturestoreReadFeaturesList getFeaturesList() {
    return new FeaturestoreReadFeaturesList();
  }

  /**
   * Gets a list of all training datasets in a featurestore
   *
   * @return a java object with parameters for reading the list of available training datasets in the featurestore.
   * The operation can be started with read() on the object and parameters can be updated with setters.
   */
  public static FeaturestoreReadTrainingDatasets getTrainingDatasets() {
    return new FeaturestoreReadTrainingDatasets();
  }

  /**
   * Gets the HDFS path to a training dataset with a specific name and version in a featurestore
   *
   * @return the hdfs path to the training dataset. The operation can be started with read() on the object and
   * parameters can be updated with setters.
   */
  public static FeaturestoreReadTrainingDatasetPath getTrainingDatasetPath(String trainingDataset) {
    return new FeaturestoreReadTrainingDatasetPath(trainingDataset);
  }

  /**
   * Gets the metadata for the specified featurestore from the metastore
   *
   * @return a java object with parameters for getting the featurestore metadata. The operation
   * can be started with read() on the object and parameters can be updated with setters
   */
  public static FeaturestoreReadMetadata getFeaturestoreMetadata() {
    return new FeaturestoreReadMetadata();
  }

  /**
   * Updates the featurestore metadata cache
   *
   * @return a java object with parameters for updating the metadata cache of a particular featurestore. The operation
   * can be started with read() on the object and parameters can be updated with setters.
   */
  public static FeaturestoreUpdateMetadataCache updateFeaturestoreMetadataCache() {
    return new FeaturestoreUpdateMetadataCache();
  }

  /**
   * Creates a new featuregroup from a spark dataframe
   *
   * @param featuregroup        the name of the featuregroup
   * @return a java object with parameters for creating a new featuregroup. The operation
   * can be started with write() on the object and parameters can be updated with setters.
   */
  public static FeaturestoreCreateFeaturegroup createFeaturegroup(String featuregroup) {
    return new FeaturestoreCreateFeaturegroup(featuregroup);
  }

  /**
   * Creates a new training dataset from a spark dataframe, saves metadata about the training dataset to the database
   * and saves the materialized dataset on hdfs
   *
   * @param trainingDataset        the name of the training dataset
   * @return a java object with parameters for creating the new training dataset. The operation
   * can be started with write() on the object and parameters can be updated with setters.
   */
  public static FeaturestoreCreateTrainingDataset createTrainingDataset(String trainingDataset) {
    return new FeaturestoreCreateTrainingDataset(trainingDataset);
  }

  /**
   * Gets the latest version of a feature group in the feature store, returns 0 if no version exists
   *
   * @param featuregroupName the name of the featuregroup to get the latest version of
   * @return a java object with parameters for getting the latest version of a featuregroup. The operation
   * can be started with read() on the object and parameters can be updated with setters.
   */
  public static FeaturestoreReadFeaturegroupLatestVersion getLatestFeaturegroupVersion(
    String featuregroupName){
    return new FeaturestoreReadFeaturegroupLatestVersion(featuregroupName);
  }

  /**
   * Gets the latest version of a training dataset in the feature store, returns 0 if no version exists
   *
   * @param trainingDatasetName the name of the trainingDataset to get the latest version of.
   * @return a java object with parameters for getting the latest version of a training dataset. The operation
   * can be started with read() on the object and parameters can be updated with setters.
   */
  public static FeaturestoreReadTrainingDatasetLatestVersion getLatestTrainingDatasetVersion(String trainingDatasetName)
  {
    return new FeaturestoreReadTrainingDatasetLatestVersion(trainingDatasetName);
  }

  /**
   * Finds the spark session dynamically if it is not provided by the user-request
   *
   * @return spark session
   */
  public static SparkSession findSpark() {
    return SparkSession.builder().enableHiveSupport().getOrCreate();
  }

  private static class InsecureHostnameVerifier implements HostnameVerifier {

    static InsecureHostnameVerifier INSTANCE = new InsecureHostnameVerifier();

    InsecureHostnameVerifier() {
    }

    @Override
    public boolean verify(String string, SSLSession ssls) {
      return string.equals(restEndpoint.split(":")[0]);
    }
  }

}
