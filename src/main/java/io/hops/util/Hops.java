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

import io.hops.util.exceptions.CredentialsNotFoundException;
import io.hops.util.exceptions.SchemaNotFoundException;
import io.hops.util.exceptions.HTTPSClientInitializationException;
import io.hops.util.exceptions.TopicNotFoundException;
import com.google.common.io.ByteStreams;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import io.hops.util.flink.FlinkConsumer;
import io.hops.util.flink.FlinkProducer;
import io.hops.util.spark.SparkConsumer;
import io.hops.util.spark.SparkProducer;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.net.util.Base64;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONObject;

/**
 * Utility class to be used by applications that want to communicate with Hopsworks.
 * Users can call the getters within their Hopsworks jobs to get the provided properties.
 *
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
  private static List<String> topics;
  private static List<String> consumerGroups;
  private static String elasticEndPoint;
  private static SparkInfo sparkInfo;

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
      try {
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
        if (sysProps.containsKey(Constants.KAFKA_TOPICS_ENV_VAR)) {
          topics = Arrays.asList(sysProps.getProperty(Constants.KAFKA_TOPICS_ENV_VAR).split(File.pathSeparator));
        }
        if (sysProps.containsKey(Constants.KAFKA_CONSUMER_GROUPS)) {
          consumerGroups = Arrays.
              asList(sysProps.getProperty(Constants.KAFKA_CONSUMER_GROUPS).split(File.pathSeparator));
        }
        sparkInfo = new SparkInfo(jobName);
      } catch (CredentialsNotFoundException ex) {
        LOG.log(Level.SEVERE, "Could not get credentials for certificates", ex);
      }
    }
  }

  /**
   * Setup the Kafka instance.
   * Endpoint is where the REST API listens for requests. I.e.
   * http://localhost:8080/. Similarly set domain to "localhost"
   * <p>
   * @param endpoint HopsWorks HTTP REST endpoint.
   * @param domain HopsWorks domain.
   * @return Hops instance.
   */
  public synchronized Hops setup(String endpoint, String domain) {
    Properties sysProps = System.getProperties();

    //validate arguments first
    this.projectId = Integer.parseInt(sysProps.getProperty(Constants.PROJECTID_ENV_VAR));
    this.projectName = sysProps.getProperty(Constants.PROJECTNAME_ENV_VAR);
    parseBrokerEndpoints(sysProps.getProperty(Constants.KAFKA_BROKERADDR_ENV_VAR));
    this.restEndpoint = endpoint + File.separator + Constants.HOPSWORKS_REST_RESOURCE;
    this.keyStore = Constants.K_CERTIFICATE_ENV_VAR;
    this.trustStore = Constants.T_CERTIFICATE_ENV_VAR;
    return this;
  }

  /**
   * Setup the Kafka instance.
   * Endpoint is where the REST API listens for requests. I.e.
   * http://localhost:8080/. Similarly set domain to "localhost"
   * KeyStore and TrustStore locations should on the local machine.
   *
   * @param topic Kafka topic name.
   * @param restEndpoint HopsWorks HTTP REST endpoint.
   * @param keyStore keystore location.
   * @param trustStore truststore location.
   * @param domain HopsWorks domain.
   * @return Hops instance.
   */
  public synchronized Hops setup(String topic, String restEndpoint,
      String keyStore,
      String trustStore, String domain) {
    Properties sysProps = System.getProperties();

    //validate arguments first
    this.projectId = Integer.parseInt(sysProps.getProperty(Constants.PROJECTID_ENV_VAR));
    this.projectName = sysProps.getProperty(Constants.PROJECTNAME_ENV_VAR);
    parseBrokerEndpoints(sysProps.getProperty(Constants.KAFKA_BROKERADDR_ENV_VAR));
    this.restEndpoint = restEndpoint + File.separator + Constants.HOPSWORKS_REST_RESOURCE;
    this.keyStore = keyStore;
    this.trustStore = trustStore;
    return this;
  }

  /**
   * Setup the Kafka instance.Endpoint is where the REST API listens for
   * requests. I.e.
   * http://localhost:8080/. Similarly set domain to "localhost"
   * KeyStore and TrustStore locations should on the local machine.
   *
   * @param pId HopsWorks project ID.
   * @param topicN Kafka topic names.
   * @param brokerE Kafka broker addresses.
   * @param restE HopsWorks HTTP REST endpoint.
   * @param keySt Keystore location.
   * @param trustSt Truststore location.
   * @param keystPwd Keystore password.
   * @param truststPwd Truststore password.
   */
  public static synchronized void setup(int pId,
      String topicN, String brokerE, String restE,
      String keySt, String trustSt, String keystPwd,
      String truststPwd) {
    parseBrokerEndpoints(brokerE);
    restEndpoint = restE;
    keyStore = keySt;
    trustStore = trustSt;
    keystorePwd = keystPwd;
    truststorePwd = truststPwd;
    projectId = pId;
    topics = new LinkedList();
    topics.add(topicN);
  }

  /**
   * Setup the Kafka instance.Endpoint is where the REST API listens for
   * requests. I.e.
   * http://localhost:8080/. Similarly set domain to "localhost"
   * KeyStore and TrustStore locations should on the local machine.
   *
   * @param projectId HopsWorks project ID.
   * @param topics Kafka topic names.
   * @param consumerGroups Kafka project consumer groups.
   * @param brokerEndpoint Kafka broker addresses.
   * @param restEndpoint HopsWorks HTTP REST endpoint.
   * @param keyStore keystore location.
   * @param trustStore truststore location.
   * @param keystorePwd Keystore password.
   * @param truststorePwd Truststore password.
   * @return Hops instance.
   */
  public synchronized Hops setup(int projectId,
      String topics, String consumerGroups, String brokerEndpoint,
      String restEndpoint,
      String keyStore, String trustStore, String keystorePwd,
      String truststorePwd) {
    this.projectId = projectId;
    parseBrokerEndpoints(brokerEndpoint);
    this.restEndpoint = restEndpoint;
    this.topics = Arrays.asList(topics.split(File.pathSeparator));
    this.consumerGroups = Arrays.
        asList(consumerGroups.split(File.pathSeparator));
    this.keyStore = keyStore;
    this.trustStore = trustStore;
    this.keystorePwd = keystorePwd;
    this.truststorePwd = truststorePwd;
    return this;
  }

  /**
   * Setup the Kafka instance by using a Map of parameters.
   *
   * @param params params
   */
  public static synchronized void setup(Map<String, String> params) {
    projectId = Integer.parseInt(params.get(Constants.PROJECTID_ENV_VAR));
    parseBrokerEndpoints(params.get(Constants.KAFKA_BROKERADDR_ENV_VAR));
    restEndpoint = params.get(Constants.HOPSWORKS_RESTENDPOINT);
    topics = Arrays.asList(params.get(Constants.KAFKA_TOPICS_ENV_VAR).split(
        File.pathSeparator));
    if (params.containsKey(Constants.KAFKA_CONSUMER_GROUPS)) {
      consumerGroups = Arrays.asList(params.get(Constants.KAFKA_CONSUMER_GROUPS).split(
          File.pathSeparator));
    }
    keyStore = params.get(Constants.K_CERTIFICATE_ENV_VAR);
    trustStore = params.get(Constants.T_CERTIFICATE_ENV_VAR);
  }

  /**
   * Get a default KafkaProperties instance.
   *
   * @return KafkaProperties.
   */
  public static KafkaProperties getKafkaProperties() {
    return new KafkaProperties();
  }

  /**
   * Get a HopsConsumer for a specific Kafka topic.
   *
   * @param topic Kafka topic name
   * @return HopsConsumer
   * @throws SchemaNotFoundException SchemaNotFoundException
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   */
  public static HopsConsumer getHopsConsumer(String topic) throws
      SchemaNotFoundException, CredentialsNotFoundException {
    return new HopsConsumer(topic);
  }

  /**
   * Get a HopsProducerfor a specific Kafka topic.
   *
   * @param topic Kafka topic name.
   * @return HopsProducer.
   * @throws SchemaNotFoundException SchemaNotFoundException
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   */
  public static HopsProducer getHopsProducer(String topic) throws
      SchemaNotFoundException, CredentialsNotFoundException {
    return new HopsProducer(topic, null);
  }

  /**
   * Get a FlinkConsumer for a specific Kafka topic.
   *
   * @param topic Kafka topic name.
   * @return FlinkConsumer.
   */
  public static FlinkConsumer getFlinkConsumer(String topic) {
    return getFlinkConsumer(topic, new AvroDeserializer(topic));
  }

  /**
   * Get a FlinkConsumer for a specific Kafka topic and DeserializationSchema.
   *
   * @param topic Kafka topic name.
   * @param deserializationSchema deserializationSchema.
   * @return FlinkConsumer.
   */
  public static FlinkConsumer getFlinkConsumer(String topic,
      DeserializationSchema deserializationSchema) {
    return new FlinkConsumer(topic, deserializationSchema,
        getKafkaProperties().getConsumerConfig());
  }

  /**
   * Get a FlinkProducer for a specific Kafka topic.
   *
   * @param topic Kafka topic name.
   * @return FlinkProducer.
   */
  public static FlinkProducer getFlinkProducer(String topic) {
    return getFlinkProducer(topic, new AvroDeserializer(topic));
  }

  /**
   * Get a FlinkProducer for a specific Kafka topic and SerializationSchema.
   *
   * @param topic Kafka topic name.
   * @param serializationSchema serializationSchema
   * @return FlinkProducer.
   */
  public static FlinkProducer getFlinkProducer(String topic,
      SerializationSchema serializationSchema) {
    return new FlinkProducer(topic, serializationSchema,
        getKafkaProperties().defaultProps());
  }

  /**
   * Helper constructor for a Spark Kafka Producer that sets the single selected topic.
   *
   * @return SparkProducer
   * @throws SchemaNotFoundException SchemaNotFoundException
   * @throws TopicNotFoundException TopicNotFoundException
   * @throws io.hops.util.exceptions.CredentialsNotFoundException CredentialsNotFoundException
   */
  public static SparkProducer getSparkProducer() throws
      SchemaNotFoundException, TopicNotFoundException, CredentialsNotFoundException {
    if (Hops.getTopics() != null && Hops.getTopics().size() == 1) {
      return new SparkProducer(Hops.getTopics().get(0), null);
    } else {
      throw new TopicNotFoundException(
          "No topic was found for this spark producer");
    }
  }

  /**
   * Get a SparkProducer for a specific Kafka topic.
   *
   * @param topic Kafka topic name.
   * @return SparkProducer
   * @throws SchemaNotFoundException SchemaNotFoundException
   * @throws io.hops.util.exceptions.CredentialsNotFoundException CredentialsNotFoundException
   */
  public static SparkProducer getSparkProducer(String topic) throws SchemaNotFoundException,
      CredentialsNotFoundException {
    return new SparkProducer(topic, null);
  }

  /**
   * Get a SparkProducer for a specific Kafka topic and extra users properties.
   *
   * @param topic Kafka topic name.
   * @param userProps User-provided Spark properties.
   * @return SparkProducer
   * @throws SchemaNotFoundException SchemaNotFoundException
   * @throws io.hops.util.exceptions.CredentialsNotFoundException CredentialsNotFoundException
   */
  public static SparkProducer getSparkProducer(String topic, Properties userProps) throws SchemaNotFoundException,
      CredentialsNotFoundException {
    return new SparkProducer(topic, userProps);
  }

  /**
   * Get a SparkConsumer for default Kafka topics, picked up from Hopsworks.
   *
   * @return SparkConsumer.
   */
  public static SparkConsumer getSparkConsumer() {
    return new SparkConsumer();
  }

  /**
   * Get a SparkConsumer and set extra user properties.
   *
   * @param userProps User-provided Spark properties.
   * @return SparkConsumer.
   */
  public static SparkConsumer getSparkConsumer(Properties userProps) {
    return new SparkConsumer(userProps);
  }

  /**
   * Get a SparkConsumer for specific Kafka topics.
   *
   * @param topics Kafka topic names.
   * @return SparkConsumer.
   * @throws TopicNotFoundException TopicNotFoundException.
   */
  public static SparkConsumer getSparkConsumer(Collection<String> topics) throws TopicNotFoundException {
    if (topics != null && !topics.isEmpty()) {
      return new SparkConsumer(topics);
    } else {
      throw new TopicNotFoundException(
          "No topic was found for this spark consumer");
    }
  }

  /**
   * Get a SparkConsumer for a specific JavaStreamingContext.
   *
   * @param jsc JavaStreamingContext
   * @return SparkConsumer
   * @throws TopicNotFoundException TopicNotFoundException
   */
  public static SparkConsumer getSparkConsumer(JavaStreamingContext jsc) throws TopicNotFoundException {
    if (topics != null && !topics.isEmpty()) {
      return new SparkConsumer(jsc, topics);
    } else {
      throw new TopicNotFoundException(
          "No topic was found for this spark consumer");
    }
  }

  /**
   * Get a SparkConsumer for a specific JavaStreamingContext and extra user properties.
   *
   * @param jsc JavaStreamingContext
   * @param userProps User-provided Spark properties.
   * @return SparkConsumer
   * @throws TopicNotFoundException TopicNotFoundException.
   */
  public static SparkConsumer getSparkConsumer(JavaStreamingContext jsc, Properties userProps) throws
      TopicNotFoundException {
    if (topics != null && !topics.isEmpty()) {
      return new SparkConsumer(jsc, topics, userProps);
    } else {
      throw new TopicNotFoundException(
          "No topic was found for this spark consumer");
    }
  }

  /**
   * Get a SparkConsumer for a specific JavaStreamingContext and specific Kafka properties.
   *
   * @param jsc JavaStreamingContext.
   * @param topics Kafka topic names.
   * @return SparkConsumer.
   */
  public static SparkConsumer getSparkConsumer(JavaStreamingContext jsc, Collection<String> topics) {
    return new SparkConsumer(jsc, topics);
  }

  /**
   * Get a SparkConsumer for a specific JavaStreamingContext, specific Kafka properties and extra users properties.
   *
   * @param jsc JavaStreamingContext.
   * @param topics Kafka topic names.
   * @param userProps User-provided Spark properties.
   * @return SparkConsumer.
   */
  public static SparkConsumer getSparkConsumer(JavaStreamingContext jsc,
      Collection<String> topics, Properties userProps) {
    return new SparkConsumer(jsc, topics, userProps);
  }

  /**
   * Get the AvroDesiarilzer for a Kafka topic.
   *
   * @param topic Kafka topic name.
   * @return AvroDeserializer.
   */
  public AvroDeserializer getHopsAvroSchema(String topic) {
    return new AvroDeserializer(topic);
  }

  /**
   * Get the Avro schema for a particular Kafka topic.
   *
   * @param topic Kafka topic name.
   * @return Avro schema as String object in JSON format
   * @throws SchemaNotFoundException SchemaNotFoundException
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   */
  public static String getSchema(String topic) throws SchemaNotFoundException, CredentialsNotFoundException {
    return getSchema(topic, Integer.MIN_VALUE);
  }

  /**
   * Get Avro Schemas for all Kafka topics directly using topics retrieved from Hopsworks.
   *
   * @return Map of schemas.
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws SchemaNotFoundException SchemaNotFoundException
   */
  public static Map<String, Schema> getSchemas() throws CredentialsNotFoundException, SchemaNotFoundException {
    Map<String, Schema> schemas = new HashMap<>();
    for (String topic : Hops.getTopics()) {
      Schema.Parser parser = new Schema.Parser();
      schemas.put(topic, parser.parse(getSchema(topic)));
    }
    return schemas;
  }

  /**
   * Get record Injections for Avro messages, using topics extracted from Spark
   * HopsWorks Jobs UI.
   *
   * @return schemas as com.twitter.bijection.Injection
   */
  public static Map<String, Injection<GenericRecord, byte[]>> getRecordInjections() {
    Map<String, Schema> schemas = null;
    try {
      schemas = Hops.getSchemas();
    } catch (CredentialsNotFoundException | SchemaNotFoundException e) {
      LOG.log(Level.SEVERE, e.getMessage());
    }
    if (schemas == null || schemas.isEmpty()) {
      return null;
    }
    Map<String, Injection<GenericRecord, byte[]>> recordInjections
        = new HashMap<>();
    for (String topic : Hops.getTopics()) {
      recordInjections.
          put(topic, GenericAvroCodecs.toBinary(schemas.get(topic)));

    }
    return recordInjections;
  }

  /**
   * Get the Avro schema for a particular Kafka topic and its version.
   *
   * @param topic Kafka topic name.
   * @param versionId Schema version ID
   * @return Avro schema as String object in JSON format
   * @throws SchemaNotFoundException SchemaNotFoundException
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   */
  public static String getSchema(String topic, int versionId) throws
      CredentialsNotFoundException, SchemaNotFoundException {
    LOG.log(Level.FINE, "Getting schema for topic:{0} from uri:{1}", new String[]{topic});
  
    JSONObject json = new JSONObject();
    json.append("topicName", topic);
    if (versionId > 0) {
      json.append("version", versionId);
    }
    Response response = null;
    try {
      response = clientWrapper(json, "schema");
    } catch (HTTPSClientInitializationException e) {
      throw new SchemaNotFoundException(e.getMessage());
    }
    LOG.log(Level.INFO,"******* response.getStatusInfo():"+response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      throw new SchemaNotFoundException("No schema found for topic:" + topic);
    }
    final String responseEntity = response.readEntity(String.class);
    //Extract fields from json
    json = new JSONObject(responseEntity);
    return json.getString("contents");
  }
  
  protected static Response clientWrapper(JSONObject json, String resource) throws CredentialsNotFoundException,
      HTTPSClientInitializationException {
    json.append(Constants.JSON_KEYSTOREPWD, keystorePwd);
    try {
      json.append(Constants.JSON_KEYSTORE, keystoreEncode());
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, null, ex);
      throw new CredentialsNotFoundException("Could not initialize Hops properties.");
    }
    
    Client client;
    try {
      client = initClient();
    } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
      throw new HTTPSClientInitializationException(e.getMessage());
    }
    WebTarget webTarget = client.target(Hops.getRestEndpoint() + "/").path(Constants.HOPSWORKS_REST_RESOURCE
        + "/"
        + Constants.HOPSWORKS_REST_APPSERVICE
        + "/" + resource);
    LOG.info("webTarget.getUri().getHost():" + webTarget.getUri().getHost());
    LOG.info("webTarget.getUri().getPort():" + webTarget.getUri().getPort());
    LOG.info("webTarget.getUri().getPath():" + webTarget.getUri().getPath());
    Invocation.Builder invocationBuilder = webTarget.request().accept(MediaType.APPLICATION_JSON);
    return invocationBuilder.post(Entity.entity(json.toString(), MediaType.APPLICATION_JSON));
  }
  

  /**
   * Get keystore password from local container.
   *
   * @return Certificate password.
   * @throws CredentialsNotFoundException CredentialsNotFoundException.
   */
  private static String getCertPw() throws CredentialsNotFoundException {
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

  /////////////////////////////////////////////
  static String keystoreEncode() throws IOException {
    FileInputStream kfin = new FileInputStream(new File(keyStore));
    byte[] kStoreBlob = ByteStreams.toByteArray(kfin);
    return Base64.encodeBase64String(kStoreBlob);
  }

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
   * Get a list of Topics set for this job.
   *
   * @return List of String object with Kafka topic names.
   */
  public static List<String> getTopics() {
    return topics;
  }

  /**
   * Get a list of Topics set for this job in a comma-separated value text format.
   *
   * @return String object with Kafka topic names in CSV format.
   */
  public static String getTopicsAsCSV() {
    StringBuilder sb = new StringBuilder();
    topics.forEach((topic) -> sb.append(topic).append(","));
    //Delete last comma
    if (sb.charAt(sb.length() - 1) == ',') {
      return sb.substring(0, sb.length() - 1);
    }
    return sb.toString();
  }

  /**
   * Get a List of Kafka consumer groups for this object.
   *
   * @return Kafka consumer groups.
   */
  public static List<String> getConsumerGroups() {
    return consumerGroups;
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
   * Shutdown gracefully a streaming spark job.
   *
   * @param jssc The JavaStreamingContext of the current application.
   * @throws InterruptedException InterruptedException
   */
  public static void shutdownGracefully(JavaStreamingContext jssc) throws InterruptedException {
    shutdownGracefully(jssc, 3000);
  }

  /**
   * Shutdown gracefully a streaming spark job.
   *
   * @param query StreamingQuery to wait on and then shutdown spark context gracefully.
   * @throws InterruptedException InterruptedException
   * @throws StreamingQueryException StreamingQueryException
   */
  public static void shutdownGracefully(StreamingQuery query) throws InterruptedException, StreamingQueryException {
    shutdownGracefully(query, 3000);
  }

  /**
   * Shutdown gracefully a streaming spark job.
   *
   * @param jssc The JavaStreamingContext of the current application.
   * @param intervalMillis How often to check if spark context has been stopped.
   * @throws InterruptedException InterruptedException
   */
  public static void shutdownGracefully(JavaStreamingContext jssc, int intervalMillis)
      throws InterruptedException {
    boolean isStopped = false;
    while (!isStopped) {
      isStopped = jssc.awaitTerminationOrTimeout(intervalMillis);
      if (!isStopped && sparkInfo.isShutdownRequested()) {
        LOG.info("Marker file has been removed, will attempt to stop gracefully the spark streaming context");
        jssc.stop(true, true);
      }
    }
  }

  /**
   * Shutdown gracefully a streaming spark job and wait for specific amount of time before exiting.
   *
   * @param query StreamingQuery to wait on and then shutdown spark context gracefully.
   * @param checkIntervalMillis whether the query has terminated or not within the checkIntervalMillis milliseconds.
   * @throws StreamingQueryException StreamingQueryException
   */
  public static void shutdownGracefully(StreamingQuery query, long checkIntervalMillis) throws
      StreamingQueryException {
    boolean isStopped = false;
    while (!isStopped) {
      isStopped = query.awaitTermination(checkIntervalMillis);
      if (!isStopped && sparkInfo.isShutdownRequested()) {
        LOG.info("Marker file has been removed, will attempt to stop gracefully the spark structured streaming query");
        query.stop();
      }
    }
  }

  /**
   * Shutdown gracefully non streaming jobs.
   *
   * @param jsc The JavaSparkContext of the current spark Job.
   * @throws InterruptedException InterruptedException
   */
  public static void shutdownGracefully(JavaSparkContext jsc) throws InterruptedException {
    while (!sparkInfo.isShutdownRequested()) {
      Thread.sleep(5000);
      LOG.info("Sleeping marker");
    }
    LOG.info("Marker file has been removed, will attempt to gracefully stop the spark context");
    jsc.stop();
    jsc.close();
  }

  /**
   * Utility method for Flink applications that need to parse Flink system variables. This method will be removed
   * when HopsWorks upgrades to latest Flink (as of time of writing, Flink 1.4).
   *
   * @param propsStr user job parameters.
   * @return Flink Kafka properties.
   */
  public static Map<String, String> getFlinkKafkaProps(String propsStr) {
    propsStr = propsStr.replace("-D", "").replace("\"", "").replace("'", "");
    Map<String, String> props = new HashMap<>();
    String[] propsArray = propsStr.split(",");
    for (String kafkaProperty : propsArray) {
      String[] keyVal = kafkaProperty.split("=");
      props.put(keyVal[0], keyVal[1]);
    }
    return props;
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
  
  private static Client initClient() throws KeyStoreException, IOException,
      NoSuchAlgorithmException,
      CertificateException {
    KeyStore truststore = KeyStore.getInstance(KeyStore.getDefaultType());
    
    try (FileInputStream trustStoreIS = new FileInputStream(Constants.DOMAIN_CA_TRUSTSTORE)) {
      truststore.load(trustStoreIS, null);
    }
    return ClientBuilder.newBuilder().trustStore(truststore).
        hostnameVerifier(InsecureHostnameVerifier.INSTANCE).build();
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
