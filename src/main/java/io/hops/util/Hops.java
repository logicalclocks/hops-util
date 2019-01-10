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

import com.google.common.io.ByteStreams;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import io.hops.util.exceptions.CredentialsNotFoundException;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturegroupCreationError;
import io.hops.util.exceptions.FeaturegroupDeletionError;
import io.hops.util.exceptions.FeaturegroupUpdateStatsError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.FeaturestoresNotFound;
import io.hops.util.exceptions.HTTPSClientInitializationException;
import io.hops.util.exceptions.InvalidPrimaryKeyForFeaturegroup;
import io.hops.util.exceptions.SchemaNotFoundException;
import io.hops.util.exceptions.SparkDataTypeNotRecognizedError;
import io.hops.util.exceptions.TopicNotFoundException;
import io.hops.util.exceptions.TrainingDatasetCreationError;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetFormatNotSupportedError;
import io.hops.util.featurestore.FeaturegroupsAndTrainingDatasetsDTO;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.feature.FeatureDTO;
import io.hops.util.featurestore.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.stats.StatisticsDTO;
import io.hops.util.featurestore.trainingdataset.TrainingDatasetDTO;
import io.hops.util.flink.FlinkConsumer;
import io.hops.util.flink.FlinkProducer;
import io.hops.util.spark.SparkConsumer;
import io.hops.util.spark.SparkProducer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.net.util.Base64;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
   *
   * @param endpoint HopsWorks HTTP REST endpoint.
   * @param domain   HopsWorks domain.
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
   * @param topic        Kafka topic name.
   * @param restEndpoint HopsWorks HTTP REST endpoint.
   * @param keyStore     keystore location.
   * @param trustStore   truststore location.
   * @param domain       HopsWorks domain.
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
   * @param pId        HopsWorks project ID.
   * @param topicN     Kafka topic names.
   * @param brokerE    Kafka broker addresses.
   * @param restE      HopsWorks HTTP REST endpoint.
   * @param keySt      Keystore location.
   * @param trustSt    Truststore location.
   * @param keystPwd   Keystore password.
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
   * @param projectId      HopsWorks project ID.
   * @param topics         Kafka topic names.
   * @param consumerGroups Kafka project consumer groups.
   * @param brokerEndpoint Kafka broker addresses.
   * @param restEndpoint   HopsWorks HTTP REST endpoint.
   * @param keyStore       keystore location.
   * @param trustStore     truststore location.
   * @param keystorePwd    Keystore password.
   * @param truststorePwd  Truststore password.
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
   * @throws SchemaNotFoundException      SchemaNotFoundException
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
   * @throws SchemaNotFoundException      SchemaNotFoundException
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
   * @param topic                 Kafka topic name.
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
   * @param topic               Kafka topic name.
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
   * @throws SchemaNotFoundException                              SchemaNotFoundException
   * @throws TopicNotFoundException                               TopicNotFoundException
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
   * @throws SchemaNotFoundException                              SchemaNotFoundException
   * @throws io.hops.util.exceptions.CredentialsNotFoundException CredentialsNotFoundException
   */
  public static SparkProducer getSparkProducer(String topic) throws SchemaNotFoundException,
      CredentialsNotFoundException {
    return new SparkProducer(topic, null);
  }

  /**
   * Get a SparkProducer for a specific Kafka topic and extra users properties.
   *
   * @param topic     Kafka topic name.
   * @param userProps User-provided Spark properties.
   * @return SparkProducer
   * @throws SchemaNotFoundException                              SchemaNotFoundException
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
   * @param jsc       JavaStreamingContext
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
   * @param jsc    JavaStreamingContext.
   * @param topics Kafka topic names.
   * @return SparkConsumer.
   */
  public static SparkConsumer getSparkConsumer(JavaStreamingContext jsc, Collection<String> topics) {
    return new SparkConsumer(jsc, topics);
  }

  /**
   * Get a SparkConsumer for a specific JavaStreamingContext, specific Kafka properties and extra users properties.
   *
   * @param jsc       JavaStreamingContext.
   * @param topics    Kafka topic names.
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
   * @throws SchemaNotFoundException      SchemaNotFoundException
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
   * @throws SchemaNotFoundException      SchemaNotFoundException
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
   * @param topic     Kafka topic name.
   * @param versionId Schema version ID
   * @return Avro schema as String object in JSON format
   * @throws SchemaNotFoundException      SchemaNotFoundException
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
      response = clientWrapper(json, "schema", HttpMethod.POST);
    } catch (HTTPSClientInitializationException e) {
      throw new SchemaNotFoundException(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      throw new SchemaNotFoundException("No schema found for topic:" + topic);
    }
    final String responseEntity = response.readEntity(String.class);
    //Extract fields from json
    json = new JSONObject(responseEntity);
    return json.getString("contents");
  }

  protected static Response clientWrapper(JSONObject json, String resource, String httpMethod)
      throws CredentialsNotFoundException,
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
    if (httpMethod.equals(HttpMethod.PUT))
      return invocationBuilder.put(Entity.entity(json.toString(), MediaType.APPLICATION_JSON));
    else
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
   * @throws StreamingQueryException StreamingQueryException
   */
  public static void shutdownGracefully(StreamingQuery query) throws StreamingQueryException {
    shutdownGracefully(query, 3000);
  }

  /**
   * Shutdown gracefully a streaming spark job.
   *
   * @param jssc           The JavaStreamingContext of the current application.
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
    propsStr = propsStr.replace("-D", "")
        .replace("\"", "").replace("'", "");
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


  /**
   * Gets the project's featurestore name (project_featurestore)
   *
   * @return the featurestore name (hive db)
   */
  public static String getProjectFeaturestore() {
    return FeaturestoreHelper.getProjectFeaturestore();
  }

  /**
   * Makes a REST call to Hopsworks Appservice to get metadata about a featurestore, this metadata is then used by
   * hops-util to infer how to JOIN featuregroups together etc.
   *
   * @param featurestore the featurestore to query metadata about
   * @return a list of featuregroups metadata
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoresNotFound FeaturestoresNotFound
   * @throws JAXBException JAXBException
   */
  private static FeaturegroupsAndTrainingDatasetsDTO getFeaturestoreMetadataRest(String featurestore)
      throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    LOG.log(Level.FINE, "Getting featuregroups for featurestore " + featurestore);

    JSONObject json = new JSONObject();
    json.append(Constants.JSON_FEATURESTORE_NAME, featurestore);
    Response response = null;
    try {
      response = clientWrapper(json, Constants.HOPSWORKS_REST_APPSERVICE_FEATURESTORE_RESOURCE, HttpMethod.POST);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturestoreNotFound(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      throw new FeaturestoreNotFound("Could not fetch featuregroups for featurestore:" + featurestore);
    }
    final String responseEntity = response.readEntity(String.class);

    JSONObject featurestoreMetadata = new JSONObject(responseEntity);
    return FeaturestoreHelper.parseFeaturestoreMetadataJson(featurestoreMetadata);
  }

  /**
   * Makes a REST call to Hopsworks for deleting
   * the contents of the featuregroup but keeps the featuregroup metadata
   *
   * @param featurestore        the featurestore where the featuregroup resides
   * @param featuregroup        the featuregroup to drop the contents of
   * @param featuregroupVersion the version of the featurergroup
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoresNotFound FeaturestoresNotFound
   */
  private static void deleteTableContents(
      String featurestore, String featuregroup, int featuregroupVersion)
      throws CredentialsNotFoundException, FeaturegroupDeletionError {
    LOG.log(Level.FINE, "Deleting table contents of featuregroup " + featuregroup +
        "version: " + featuregroupVersion + " in featurestore: " + featurestore);

    JSONObject json = new JSONObject();
    json.append(Constants.JSON_FEATURESTORE_NAME, featurestore);
    json.append(Constants.JSON_FEATUREGROUP_NAME, featuregroup);
    json.append(Constants.JSON_FEATUREGROUP_VERSION, featuregroupVersion);
    try {
      Response response = clientWrapper(json, Constants.HOPSWORKS_REST_APPSERVICE_CLEAR_FEATUREGROUP_RESOURCE,
          HttpMethod.POST);
      LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
      if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
        throw new FeaturegroupDeletionError("Could not clear the contents of featuregroup:" + featuregroup);
      }
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupDeletionError(e.getMessage());
    }
  }

  /**
   * Makes a REST call to Hopsworks for creating a new featuregroup from a spark dataframe.
   *
   * @param featurestore        the featurestore where the group will be created
   * @param featuregroup        the name of the featuregroup
   * @param featuregroupVersion the version of the featuregroup
   * @param description         the description of the featuregroup
   * @param jobName               the name of the job to compute the featuregroup
   * @param dependencies        a list of dependencies (datasets that this featuregroup depends on)
   * @param featuresSchema      schema of features for the featuregroup
   * @param statisticsDTO       statistics about the featuregroup
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupCreationError FeaturegroupCreationError
   */
  private static void createFeaturegroupRest(
      String featurestore, String featuregroup, int featuregroupVersion, String description,
      String jobName, List<String> dependencies, List<FeatureDTO> featuresSchema,
    StatisticsDTO statisticsDTO)
      throws CredentialsNotFoundException, JAXBException, FeaturegroupCreationError {
    LOG.log(Level.FINE, "Creating featuregroup " + featuregroup +
        " in featurestore: " + featurestore);
    JSONObject json = new JSONObject();
    json.put(Constants.JSON_FEATURESTORE_NAME, featurestore);
    json.put(Constants.JSON_FEATUREGROUP_NAME, featuregroup);
    json.put(Constants.JSON_FEATUREGROUP_VERSION, featuregroupVersion);
    json.put(Constants.JSON_FEATUREGROUP_DESCRIPTION, description);
    json.put(Constants.JSON_FEATUREGROUP_JOBNAME, jobName);
    json.put(Constants.JSON_FEATUREGROUP_DEPENDENCIES, dependencies);
    json.put(Constants.JSON_FEATUREGROUP_FEATURES,
        FeaturestoreHelper.convertFeatureDTOsToJsonObjects(featuresSchema));
    json.put(Constants.JSON_FEATUREGROUP_FEATURE_CORRELATION,
        FeaturestoreHelper.convertFeatureCorrelationMatrixDTOToJsonObject(
            statisticsDTO.getFeatureCorrelationMatrixDTO()));
    json.put(Constants.JSON_FEATUREGROUP_DESC_STATS,
        FeaturestoreHelper.convertDescriptiveStatsDTOToJsonObject(statisticsDTO.getDescriptiveStatsDTO()));
    json.put(Constants.JSON_FEATUREGROUP_FEATURES_HISTOGRAM, FeaturestoreHelper
        .convertFeatureDistributionsDTOToJsonObject(statisticsDTO.getFeatureDistributionsDTO()));
    json.put(Constants.JSON_FEATUREGROUP_CLUSTER_ANALYSIS, FeaturestoreHelper
        .convertClusterAnalysisDTOToJsonObject(statisticsDTO.getClusterAnalysisDTO()));
    json.put(Constants.JSON_FEATUREGROUP_UPDATE_METADATA, false);
    json.put(Constants.JSON_FEATUREGROUP_UPDATE_STATS, false);
    Response response;
    try {
      response = clientWrapper(json, Constants.HOPSWORKS_REST_APPSERVICE_CREATE_FEATUREGROUP_RESOURCE, HttpMethod.POST);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupCreationError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.CREATED.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = parseHopsworksErrorResponse(response);
      throw new FeaturegroupCreationError("Could not create featuregroup:" + featuregroup +
          " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
          + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
  }

  /**
   * Makes a REST call to Hopsworks for creating a new training dataset from a spark dataframe
   *
   * @param featurestore           the featurestore where the group will be created
   * @param trainingDataset
   * @param trainingDatasetVersion the version of the featuregroup
   * @param description            the description of the featuregroup
   * @param jobName                  the name of the job to compute the featuregroup
   * @param dependencies           a list of dependencies (datasets that this featuregroup depends on)
   * @param featuresSchema         schema of features for the featuregroup
   * @param statisticsDTO          statistics about the featuregroup
   * @param dataFormat             format of the dataset (e.g tfrecords)
   * @return the JSON response
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws JAXBException JAXBException
   * @throws TrainingDatasetCreationError TrainingDatasetCreationError
   */
  private static Response createTrainingDatasetRest(
      String featurestore, String trainingDataset, int trainingDatasetVersion, String description,
      String jobName, String dataFormat, List<String> dependencies, List<FeatureDTO> featuresSchema,
      StatisticsDTO statisticsDTO) throws CredentialsNotFoundException, JAXBException, TrainingDatasetCreationError {
    LOG.log(Level.FINE, "Creating Training Dataset " + trainingDataset +
        " in featurestore: " + featurestore);
    JSONObject json = new JSONObject();
    json.put(Constants.JSON_FEATURESTORE_NAME, featurestore);
    json.put(Constants.JSON_TRAINING_DATASET_NAME, trainingDataset);
    json.put(Constants.JSON_TRAINING_DATASET_VERSION, trainingDatasetVersion);
    json.put(Constants.JSON_TRAINING_DATASET_DESCRIPTION, description);
    json.put(Constants.JSON_TRAINING_DATASET_JOBNAME, jobName);
    json.put(Constants.JSON_TRAINING_DATASET_DEPENDENCIES, dependencies);
    json.put(Constants.JSON_TRAINING_DATASET_FORMAT, dataFormat);
    json.put(Constants.JSON_TRAINING_DATASET_SCHEMA,
        FeaturestoreHelper.convertFeatureDTOsToJsonObjects(featuresSchema));
    json.put(Constants.JSON_TRAINING_DATASET_FEATURE_CORRELATION,
        FeaturestoreHelper.convertFeatureCorrelationMatrixDTOToJsonObject(
            statisticsDTO.getFeatureCorrelationMatrixDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_DESC_STATS,
        FeaturestoreHelper.convertDescriptiveStatsDTOToJsonObject(statisticsDTO.getDescriptiveStatsDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_FEATURES_HISTOGRAM, FeaturestoreHelper
        .convertFeatureDistributionsDTOToJsonObject(statisticsDTO.getFeatureDistributionsDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_CLUSTER_ANALYSIS, FeaturestoreHelper
        .convertClusterAnalysisDTOToJsonObject(statisticsDTO.getClusterAnalysisDTO()));
    Response response;
    try {
      response = clientWrapper(json,
          Constants.HOPSWORKS_REST_APPSERVICE_CREATE_TRAINING_DATASET_RESOURCE, HttpMethod.POST);
    } catch (HTTPSClientInitializationException e) {
      throw new TrainingDatasetCreationError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.CREATED.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = parseHopsworksErrorResponse(response);
      throw new TrainingDatasetCreationError("Could not create trainingDataset:" + trainingDataset +
          " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
          + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
    return response;
  }

  /**
   * Utility method for parsing the JSON response thrown by Hopsworks in case of errors
   *
   * @param response the JSON response to parse
   * @return a DTO with the parsed result
   */
  private static HopsworksErrorResponseDTO parseHopsworksErrorResponse(Response response) {
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
   * @return a list of names of the featurestores accessible by this project
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoresNotFound FeaturestoresNotFound
   */
  private static List<String> getFeaturestoresForProject()
      throws CredentialsNotFoundException, FeaturestoresNotFound {
    LOG.log(Level.FINE, "Getting featurestores for current project");

    JSONObject json = new JSONObject();
    Response response = null;
    try {
      response = clientWrapper(json, Constants.HOPSWORKS_REST_APPSERVICE_FEATURESTORES_RESOURCE, HttpMethod.POST);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturestoresNotFound(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      throw new FeaturestoresNotFound("Could not fetch featurestores for the current project");
    }
    final String responseEntity = response.readEntity(String.class);
    JSONArray featurestoresJson = new JSONArray(responseEntity);
    List<String> featurestores = new ArrayList();
    for (int i = 0; i < featurestoresJson.length(); i++) {
      JSONObject featurestoreJson = featurestoresJson.getJSONObject(i);
      String featurestoreName = featurestoreJson.getString(Constants.JSON_FEATURESTORE_NAME);
      featurestores.add(featurestoreName);
    }
    return featurestores;
  }

  /**
   * Inserts a spark dataframe into a featuregroup
   *
   * @param sparkDf             the spark dataframe to insert
   * @param sparkSession        the spark session
   * @param featuregroup        the name of the featuregroup to insert into
   * @param featurestore        the name of the featurestore where the featuregroup resides
   * @param featuregroupVersion the version of the featuregroup
   * @param mode                the mode to use when inserting (append/overwrite)
   * @param descriptiveStats    a boolean flag whether to compute descriptive statistics of the new data
   * @param featureCorr         a boolean flag whether to compute feature correlation analysis of the new data
   * @param featureHistograms   a boolean flag whether to compute feature histograms of the new data
   * @param clusterAnalysis     a boolean flag whether to compute cluster analysis of the new data
   * @param statColumns         a list of columns to compute statistics for (defaults to all columns that are numeric)
   * @param numBins             number of bins to use for computing histograms
   * @param corrMethod          the method to compute feature correlation with (pearson or spearman)
   * @param numClusters         number of clusters to use for cluster analysis
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturegroupDeletionError FeaturegroupDeletionError
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   */
  public static void insertIntoFeaturegroup(
      SparkSession sparkSession, Dataset<Row> sparkDf, String featuregroup,
      String featurestore, int featuregroupVersion, String mode, Boolean descriptiveStats, Boolean featureCorr,
      Boolean featureHistograms, Boolean clusterAnalysis, List<String> statColumns, Integer numBins,
      String corrMethod, Integer numClusters)
      throws CredentialsNotFoundException, FeaturegroupDeletionError, JAXBException, FeaturegroupUpdateStatsError,
      DataframeIsEmpty, SparkDataTypeNotRecognizedError {
    if (featurestore == null)
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    if (sparkSession == null)
      sparkSession = findSpark();
    if (corrMethod == null) {
      corrMethod = Constants.CORRELATION_ANALYSIS_DEFAULT_METHOD;
    }
    if (numBins == null) {
      numBins = 20;
    }
    if (numClusters == null) {
      numClusters = 5;
    }
    sparkSession.sparkContext().setJobGroup(
        "Inserting dataframe into featuregroup",
        "Inserting into featuregroup:" + featuregroup + " in the featurestore:" +
            featurestore, true);
    if (!mode.equalsIgnoreCase("append") && !mode.equalsIgnoreCase("overwrite"))
      throw new IllegalArgumentException("The supplied write mode: " + mode +
          " does not match any of the supported modes: overwrite, append");
    if (mode.equalsIgnoreCase("overwrite")) {
      deleteTableContents(featurestore, featuregroup, featuregroupVersion);
    }
    sparkSession.sparkContext().setJobGroup("", "", true);
    FeaturestoreHelper.insertIntoFeaturegroup(sparkDf, sparkSession, featuregroup,
        featurestore, featuregroupVersion);
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(featuregroup, sparkSession, sparkDf,
        featurestore, featuregroupVersion,
        descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns, numBins, numClusters,
        corrMethod);
    updateFeaturegroupStatsRest(featuregroup, featurestore, featuregroupVersion, statisticsDTO);
  }

  /**
   * Inserts a spark dataframe into an existing training dataset (append/overwrite)
   *
   * @param sparkDf                the spark dataframe to insert
   * @param sparkSession           the spark session
   * @param trainingDataset        the name of the training dataset to insert into
   * @param featurestore           the name of the featurestore where the training dataset resides
   * @param trainingDatasetVersion the version of the training dataset
   * @param descriptiveStats       a boolean flag whether to compute descriptive statistics of the new data
   * @param featureCorr            a boolean flag whether to compute feature correlation analysis of the new data
   * @param featureHistograms      a boolean flag whether to compute feature histograms of the new data
   * @param clusterAnalysis        a boolean flag whether to compute cluster analysis of the new data
   * @param statColumns            a list of columns to compute statistics for (defaults to all columns
   *                               that are numeric)
   * @param numBins                number of bins to use for computing histograms
   * @param corrMethod             the method to compute feature correlation with (pearson or spearman)
   * @param numClusters            number of clusters to use for cluster analysis
   * @param writeMode              the spark write mode (append/overwrite)
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   */
  public static void insertIntoTrainingDataset(
      SparkSession sparkSession, Dataset<Row> sparkDf, String trainingDataset,
      String featurestore, int trainingDatasetVersion,
      Boolean descriptiveStats, Boolean featureCorr,
      Boolean featureHistograms, Boolean clusterAnalysis, List<String> statColumns, Integer numBins,
      String corrMethod, Integer numClusters, String writeMode)
      throws CredentialsNotFoundException, JAXBException, FeaturestoreNotFound,
      TrainingDatasetDoesNotExistError, DataframeIsEmpty, FeaturegroupUpdateStatsError,
      TrainingDatasetFormatNotSupportedError, SparkDataTypeNotRecognizedError {
    if (featurestore == null)
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    if (sparkSession == null)
      sparkSession = findSpark();
    if (corrMethod == null) {
      corrMethod = Constants.CORRELATION_ANALYSIS_DEFAULT_METHOD;
    }
    if (numBins == null) {
      numBins = 20;
    }
    if (numClusters == null) {
      numClusters = 5;
    }
    FeaturegroupsAndTrainingDatasetsDTO featuregroupsAndTrainingDatasetsDTO = getFeaturestoreMetadata(featurestore);
    List<TrainingDatasetDTO> trainingDatasetDTOList = featuregroupsAndTrainingDatasetsDTO.getTrainingDatasets();
    FeaturestoreHelper.findTrainingDataset(trainingDatasetDTOList,
        trainingDataset, trainingDatasetVersion);
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(trainingDataset, sparkSession,
        sparkDf,
        featurestore, trainingDatasetVersion,
        descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns, numBins, numClusters,
        corrMethod);
    Response response = updateTrainingDatasetStatsRest(trainingDataset, featurestore, trainingDatasetVersion,
        statisticsDTO);
    String jsonStrResponse = response.readEntity(String.class);
    JSONObject jsonObjResponse = new JSONObject(jsonStrResponse);
    TrainingDatasetDTO updatedTrainingDatasetDTO = FeaturestoreHelper.parseTrainingDatasetJson(jsonObjResponse);
    String hdfsPath = updatedTrainingDatasetDTO.getHdfsStorePath() + "/" + trainingDataset;
    FeaturestoreHelper.writeTrainingDatasetHdfs(sparkSession, sparkDf, hdfsPath,
        updatedTrainingDatasetDTO.getDataFormat(), writeMode);
    if (updatedTrainingDatasetDTO.getDataFormat() == Constants.TRAINING_DATASET_TFRECORDS_FORMAT) {
      try {
        JSONObject tfRecordSchemaJson = FeaturestoreHelper.getDataframeTfRecordSchemaJson(sparkDf);
        FeaturestoreHelper.writeTfRecordSchemaJson(updatedTrainingDatasetDTO.getHdfsStorePath()
                + Constants.SLASH_DELIMITER + Constants.TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME,
            tfRecordSchemaJson.toString());
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Could not save tf record schema json to HDFS for training dataset: "
            + trainingDataset, e);
      }
    }
  }


  /**
   * Gets a featuregroup from a particular featurestore
   *
   * @param sparkSession        the spark session
   * @param featuregroup        the featuregroup to get
   * @param featurestore        the featurestore to query
   * @param featuregroupVersion the version of the featuregroup to get
   * @return a spark dataframe with the featuregroup
   */
  public static Dataset<Row> getFeaturegroup(SparkSession sparkSession, String featuregroup,
                                             String featurestore, int featuregroupVersion) {
    if (featurestore == null)
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    if (sparkSession == null)
      sparkSession = findSpark();
    return FeaturestoreHelper.getFeaturegroup(sparkSession, featuregroup, featurestore, featuregroupVersion);
  }

  /**
   * Gets a training dataset from a featurestore
   *
   * @param sparkSession           the spark session
   * @param trainingDataset        the training dataset to get
   * @param featurestore           the featurestore where the training dataset resides
   * @param trainingDatasetVersion the version of the training dataset
   * @return a spark dataframe with the training dataset
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws IOException IOException
   */
  public static Dataset<Row> getTrainingDataset(
      SparkSession sparkSession, String trainingDataset,
      String featurestore, int trainingDatasetVersion)
      throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException, TrainingDatasetDoesNotExistError,
      TrainingDatasetFormatNotSupportedError, IOException {
    if (featurestore == null)
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    if (sparkSession == null)
      sparkSession = findSpark();
    FeaturegroupsAndTrainingDatasetsDTO featuregroupsAndTrainingDatasetsDTO = getFeaturestoreMetadata(featurestore);
    List<TrainingDatasetDTO> trainingDatasetDTOList = featuregroupsAndTrainingDatasetsDTO.getTrainingDatasets();
    TrainingDatasetDTO trainingDatasetDTO = FeaturestoreHelper.findTrainingDataset(trainingDatasetDTOList,
        trainingDataset, trainingDatasetVersion);
    String hdfsPath = Constants.HDFS_DEFAULT + trainingDatasetDTO.getHdfsStorePath() +
        Constants.SLASH_DELIMITER + trainingDatasetDTO.getName();
    return FeaturestoreHelper.getTrainingDataset(sparkSession, trainingDatasetDTO.getDataFormat(),
        hdfsPath);
  }

  /**
   * Gets a feature from a featurestore and infers the featuregroup where the feature is located
   *
   * @param sparkSession the spark session
   * @param feature      the feature to get
   * @param featurestore the featurestore to query
   * @return A dataframe with the feature
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static Dataset<Row> getFeature(
      SparkSession sparkSession, String feature,
      String featurestore) throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    if (featurestore == null)
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    if (sparkSession == null)
      sparkSession = findSpark();
    FeaturegroupsAndTrainingDatasetsDTO featuregroupsAndTrainingDatasetsDTO = getFeaturestoreMetadata(featurestore);
    List<FeaturegroupDTO> featuregroupsMetadata = featuregroupsAndTrainingDatasetsDTO.getFeaturegroups();
    return FeaturestoreHelper.getFeature(sparkSession, feature, featurestore, featuregroupsMetadata);
  }

  /**
   * Gets a feature from a featurestore and a specific featuregroup.
   *
   * @param sparkSession        the spark session
   * @param feature             the feature to get
   * @param featurestore        the featurestore to query
   * @param featuregroup        the featuregroup where the feature is located
   * @param featuregroupVersion the version of the featuregroup
   * @return a spark dataframe with the feature
   */
  public static Dataset<Row> getFeature(SparkSession sparkSession, String feature, String featurestore,
                                        String featuregroup, int featuregroupVersion) {
    return FeaturestoreHelper.getFeature(sparkSession, feature, featurestore, featuregroup, featuregroupVersion);
  }

  /**
   * Method for updating the statistics of a featuregroup (recomputing the statistics)
   *
   * @param sparkSession        the spark session
   * @param featuregroup        the name of the featuregroup to update statistics for
   * @param featurestore        the name of the featurestore where the featuregroup resides
   * @param featuregroupVersion the version of the featuregroup
   * @param descriptiveStats    a boolean flag whether to compute descriptive statistics of the new data
   * @param featureCorr         a boolean flag whether to compute feature correlation analysis of the new data
   * @param featureHistograms   a boolean flag whether to compute feature histograms of the new data
   * @param clusterAnalysis     a boolean flag whether to compute cluster analysis of the new data
   * @param statColumns         a list of columns to compute statistics for (defaults to all columns that are numeric)
   * @param numBins             number of bins to use for computing histograms
   * @param corrMethod          the method to compute feature correlation with (pearson or spearman)
   * @param numClusters         number of clusters to use for cluster analysis
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   */
  public static void updateFeaturegroupStats(
      SparkSession sparkSession, String featuregroup, String featurestore,
      int featuregroupVersion, Boolean descriptiveStats, Boolean featureCorr,
      Boolean featureHistograms, Boolean clusterAnalysis, List<String> statColumns, Integer numBins,
      String corrMethod, Integer numClusters) throws DataframeIsEmpty, CredentialsNotFoundException, JAXBException,
      FeaturegroupUpdateStatsError, SparkDataTypeNotRecognizedError {
    if (featurestore == null) {
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    }
    if (sparkSession == null)
      sparkSession = findSpark();
    if (corrMethod == null) {
      corrMethod = Constants.CORRELATION_ANALYSIS_DEFAULT_METHOD;
    }
    if (numBins == null) {
      numBins = 20;
    }
    if (numClusters == null) {
      numClusters = 5;
    }
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(featuregroup, sparkSession, null,
        featurestore, featuregroupVersion,
        descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns, numBins, numClusters,
        corrMethod);
    updateFeaturegroupStatsRest(featuregroup, featurestore, featuregroupVersion, statisticsDTO);
  }

  /**
   * Method for updating the statistics of a training dataset (recomputing the statistics)
   *
   * @param sparkSession           the spark session
   * @param trainingDataset        the name of the training datasaet to update statistics for
   * @param featurestore           the name of the featurestore where the featuregroup resides
   * @param trainingDatasetVersion the version of the training dataset
   * @param descriptiveStats       a boolean flag whether to compute descriptive statistics of the new data
   * @param featureCorr            a boolean flag whether to compute feature correlation analysis of the new data
   * @param featureHistograms      a boolean flag whether to compute feature histograms of the new data
   * @param clusterAnalysis        a boolean flag whether to compute cluster analysis of the new data
   * @param statColumns            a list of columns to compute statistics for (defaults to all
   *                               columns that are numeric)
   * @param numBins                number of bins to use for computing histograms
   * @param corrMethod             the method to compute feature correlation with (pearson or spearman)
   * @param numClusters            number of clusters to use for cluster analysis
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws JAXBException JAXBException
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws IOException IOException
   */
  public static void updateTrainingDatasetStats(
      SparkSession sparkSession, String trainingDataset, String featurestore,
      int trainingDatasetVersion, Boolean descriptiveStats, Boolean featureCorr,
      Boolean featureHistograms, Boolean clusterAnalysis, List<String> statColumns, Integer numBins,
      String corrMethod, Integer numClusters) throws DataframeIsEmpty, CredentialsNotFoundException, JAXBException,
      FeaturegroupUpdateStatsError, SparkDataTypeNotRecognizedError, TrainingDatasetFormatNotSupportedError,
      FeaturestoreNotFound, TrainingDatasetDoesNotExistError, IOException {
    if (featurestore == null) {
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    }
    if (sparkSession == null)
      sparkSession = findSpark();
    if (corrMethod == null) {
      corrMethod = Constants.CORRELATION_ANALYSIS_DEFAULT_METHOD;
    }
    if (numBins == null) {
      numBins = 20;
    }
    if (numClusters == null) {
      numClusters = 5;
    }
    Dataset<Row> sparkDf = getTrainingDataset(sparkSession, trainingDataset, featurestore, trainingDatasetVersion);
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(trainingDataset, sparkSession, sparkDf,
        featurestore, trainingDatasetVersion,
        descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns, numBins, numClusters,
        corrMethod);
    updateTrainingDatasetStatsRest(trainingDataset, featurestore, trainingDatasetVersion, statisticsDTO);
  }

  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe. This method is used if the user
   * has itself provided a set of featuregroups where the features are located and should be queried from
   * and a join key, it does not infer the featuregroups.
   *
   * @param sparkSession             the spark session
   * @param features                 the list of features to get
   * @param featurestore             the featurestore to query
   * @param featuregroupsAndVersions a map of (featuregroup to version) where the featuregroups are located
   * @param joinKey                  the key to join on
   * @return a spark dataframe with the features
   */
  public static Dataset<Row> getFeatures(SparkSession sparkSession, List<String> features, String featurestore,
                                         Map<String, Integer> featuregroupsAndVersions, String joinKey) {
    return FeaturestoreHelper.getFeatures(sparkSession, features, featurestore, featuregroupsAndVersions, joinKey);
  }

  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe. This method is used if the user
   * has itself provided a set of featuregroups where the features are located and should be queried from
   * but not a join key, it does not infer the featuregroups but infers the join key
   *
   * @param sparkSession             the spark session
   * @param features                 the list of features to get
   * @param featurestore             the featurestore to query
   * @param featuregroupsAndVersions a map of (featuregroup to version) where the featuregroups are located
   * @return a spark dataframe with the features
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static Dataset<Row> getFeatures(
      SparkSession sparkSession, List<String> features, String featurestore,
      Map<String, Integer> featuregroupsAndVersions)
      throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    FeaturegroupsAndTrainingDatasetsDTO featuregroupsAndTrainingDatasetsDTO = getFeaturestoreMetadata(featurestore);
    List<FeaturegroupDTO> featuregroupsMetadata = featuregroupsAndTrainingDatasetsDTO.getFeaturegroups();
    List<FeaturegroupDTO> filteredFeaturegroupsMetadata =
        FeaturestoreHelper.filterFeaturegroupsBasedOnMap(featuregroupsAndVersions, featuregroupsMetadata);
    String joinKey = FeaturestoreHelper.getJoinColumn(filteredFeaturegroupsMetadata);
    return FeaturestoreHelper.getFeatures(sparkSession, features, featurestore, featuregroupsAndVersions, joinKey);
  }


  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe. This method will infer
   * in which featuregroups the features belong but uses a user-supplied join key
   *
   * @param sparkSession the spark session
   * @param features     the list of features to get
   * @param featurestore the featurestore to query
   * @param joinKey      the key to join on
   * @return a spark dataframe with the features
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static Dataset<Row> getFeatures(
      SparkSession sparkSession, List<String> features,
      String featurestore, String joinKey) throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    FeaturegroupsAndTrainingDatasetsDTO featuregroupsAndTrainingDatasetsDTO = getFeaturestoreMetadata(featurestore);
    List<FeaturegroupDTO> featuregroupsMetadata = featuregroupsAndTrainingDatasetsDTO.getFeaturegroups();
    return FeaturestoreHelper.getFeatures(sparkSession, features, featurestore, featuregroupsMetadata, joinKey);
  }

  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe. This method will infer
   * in which featuregroups the features belong and which join_key to use using metadata from the metastore
   *
   * @param sparkSession the spark session
   * @param features     the list of features to get
   * @param featurestore the featurestore to query
   * @return a spark dataframe with the features
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static Dataset<Row> getFeatures(
      SparkSession sparkSession, List<String> features,
      String featurestore) throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    FeaturegroupsAndTrainingDatasetsDTO featuregroupsAndTrainingDatasetsDTO = getFeaturestoreMetadata(featurestore);
    List<FeaturegroupDTO> featuregroupsMetadata = featuregroupsAndTrainingDatasetsDTO.getFeaturegroups();
    List<FeaturegroupDTO> featuregroupsMatching =
        FeaturestoreHelper.findFeaturegroupsThatContainsFeatures(featuregroupsMetadata, features, featurestore);
    String joinKey = FeaturestoreHelper.getJoinColumn(featuregroupsMatching);
    return FeaturestoreHelper.getFeatures(sparkSession, features, featurestore, featuregroupsMatching, joinKey);
  }

  /**
   * Runs an SQL query on the project's featurestore
   *
   * @param sparkSession the spark session
   * @param query        the query to run
   * @param featurestore the featurestore to query
   * @return the resulting Spark dataframe
   */
  public static Dataset<Row> queryFeaturestore(SparkSession sparkSession, String query, String featurestore) {
    if (featurestore == null)
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    return FeaturestoreHelper.queryFeaturestore(sparkSession, query, featurestore);
  }

  /**
   * Gets a list of featurestores accessible by the current project
   *
   * @return a list of names of the featurestores
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoresNotFound FeaturestoresNotFound
   */
  public static List<String> getProjectFeaturestores() throws CredentialsNotFoundException, FeaturestoresNotFound {
    return getFeaturestoresForProject();
  }

  /**
   * Gets a list of all featuregroups in a featurestore
   *
   * @param featurestore the featurestore to get the featuregroups for
   * @return a list of names of the feature groups
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static List<String> getFeaturegroups(String featurestore) throws
      CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    if (featurestore == null)
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    return getFeaturestoreMetadata(featurestore).
        getFeaturegroups().stream()
        .map(fg -> FeaturestoreHelper.getTableName(fg.getName(), fg.getVersion())).collect(Collectors.toList());
  }

  /**
   * Gets a list of all feature names in a featurestore
   *
   * @param featurestore the featurestore to get the features for
   * @return a list of names of the features in the feature store
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static List<String> getFeaturesList(String featurestore) throws
      CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    if (featurestore == null)
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    List<List<String>> featureNamesLists = getFeaturestoreMetadata(featurestore).
        getFeaturegroups().stream()
        .map(fg -> fg.getFeatures().stream().map(f -> f.getName())
            .collect(Collectors.toList())).collect(Collectors.toList());
    List<String> featureNames = new ArrayList<>();
    featureNamesLists.stream().forEach(flist -> featureNames.addAll(flist));
    return featureNames;
  }

  /**
   * Gets a list of all training datasets in a featurestore
   *
   * @param featurestore the featurestore to get the training datasets for
   * @return a list of names of the trainin datasets
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static List<String> getTrainingDatasets(String featurestore) throws CredentialsNotFoundException,
      FeaturestoreNotFound, JAXBException {
    if (featurestore == null)
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    return getFeaturestoreMetadata(featurestore).
        getTrainingDatasets().stream()
        .map(td -> FeaturestoreHelper.getTableName(td.getName(), td.getVersion())).collect(Collectors.toList());
  }

  /**
   * Gets the HDFS path to a training dataset with a specific name and version in a featurestore
   *
   * @param trainingDataset        name of the training dataset
   * @param featurestore           featurestore that the training dataset is linked to
   * @param trainingDatasetVersion version of the training dataset
   * @return the hdfs path to the training dataset
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static String getTrainingDatasetPath(String trainingDataset, String featurestore,
                                              int trainingDatasetVersion) throws
      TrainingDatasetDoesNotExistError, CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    if (featurestore == null)
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    FeaturegroupsAndTrainingDatasetsDTO featuregroupsAndTrainingDatasetsDTO = getFeaturestoreMetadata(featurestore);
    List<TrainingDatasetDTO> trainingDatasetDTOList = featuregroupsAndTrainingDatasetsDTO.getTrainingDatasets();
    TrainingDatasetDTO trainingDatasetDTO = FeaturestoreHelper.findTrainingDataset(trainingDatasetDTOList,
        trainingDataset, trainingDatasetVersion);
    return Constants.HDFS_DEFAULT + trainingDatasetDTO.getHdfsStorePath() +
        Constants.SLASH_DELIMITER + trainingDatasetDTO.getName();
  }

  /**
   * Gets the metadata for the specified featurestore from the metastore
   *
   * @param featurestore the featurestore to query metadata from
   * @return a list of metadata about all the featuregroups in the featurestore
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static FeaturegroupsAndTrainingDatasetsDTO getFeaturestoreMetadata(String featurestore)
      throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    if (featurestore == null)
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    return getFeaturestoreMetadataRest(featurestore);
  }

  /**
   * Makes a REST call to Hopsworks for updating the statistics of a featuregroup
   *
   * @param featuregroup        the name of the featuregroup
   * @param featurestore        the name of the featurestore where the featuregroup resides
   * @param featuregroupVersion the version of the featuregroup
   * @param statisticsDTO       the new statistics of the featuregroup
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   */
  public static void updateFeaturegroupStatsRest(
      String featuregroup, String featurestore, int featuregroupVersion, StatisticsDTO statisticsDTO)
      throws CredentialsNotFoundException,
      JAXBException, FeaturegroupUpdateStatsError {
    LOG.log(Level.FINE, "Updating featuregroup stats for: " + featuregroup +
        " in featurestore: " + featurestore);
    JSONObject json = new JSONObject();
    json.put(Constants.JSON_FEATURESTORE_NAME, featurestore);
    json.put(Constants.JSON_FEATUREGROUP_NAME, featuregroup);
    json.put(Constants.JSON_FEATUREGROUP_VERSION, featuregroupVersion);
    json.put(Constants.JSON_FEATUREGROUP_FEATURE_CORRELATION,
        FeaturestoreHelper.convertFeatureCorrelationMatrixDTOToJsonObject(
            statisticsDTO.getFeatureCorrelationMatrixDTO()));
    json.put(Constants.JSON_FEATUREGROUP_DESC_STATS,
        FeaturestoreHelper.convertDescriptiveStatsDTOToJsonObject(statisticsDTO.getDescriptiveStatsDTO()));
    json.put(Constants.JSON_FEATUREGROUP_FEATURES_HISTOGRAM, FeaturestoreHelper
        .convertFeatureDistributionsDTOToJsonObject(statisticsDTO.getFeatureDistributionsDTO()));
    json.put(Constants.JSON_FEATUREGROUP_CLUSTER_ANALYSIS, FeaturestoreHelper
        .convertClusterAnalysisDTOToJsonObject(statisticsDTO.getClusterAnalysisDTO()));
    json.put(Constants.JSON_FEATUREGROUP_UPDATE_METADATA, false);
    json.put(Constants.JSON_FEATUREGROUP_UPDATE_STATS, true);
    Response response = null;
    try {
      response = clientWrapper(json, Constants.HOPSWORKS_REST_APPSERVICE_UPDATE_FEATUREGROUP_RESOURCE, HttpMethod.PUT);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupUpdateStatsError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = parseHopsworksErrorResponse(response);
      throw new FeaturegroupUpdateStatsError("Could not update statistics for featuregroup:" + featuregroup +
          " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
          + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
  }

  /**
   * @param trainingDataset        the name of the training dataset
   * @param featurestore           the name of the featurestore where the training dataset resides
   * @param trainingDatasetVersion the version of the training dataset
   * @param statisticsDTO          the new statistics of the training dataset
   * @return the JSON response
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   */
  public static Response updateTrainingDatasetStatsRest(
      String trainingDataset, String featurestore, int trainingDatasetVersion, StatisticsDTO statisticsDTO)
      throws CredentialsNotFoundException,
      JAXBException, FeaturegroupUpdateStatsError {
    LOG.log(Level.FINE, "Updating training dataset stats for: " + trainingDataset +
        " in featurestore: " + featurestore);
    JSONObject json = new JSONObject();
    json.put(Constants.JSON_FEATURESTORE_NAME, featurestore);
    json.put(Constants.JSON_TRAINING_DATASET_NAME, trainingDataset);
    json.put(Constants.JSON_TRAINING_DATASET_VERSION, trainingDatasetVersion);
    json.put(Constants.JSON_TRAINING_DATASET_FEATURE_CORRELATION,
        FeaturestoreHelper.convertFeatureCorrelationMatrixDTOToJsonObject(
            statisticsDTO.getFeatureCorrelationMatrixDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_DESC_STATS,
        FeaturestoreHelper.convertDescriptiveStatsDTOToJsonObject(statisticsDTO.getDescriptiveStatsDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_FEATURES_HISTOGRAM, FeaturestoreHelper
        .convertFeatureDistributionsDTOToJsonObject(statisticsDTO.getFeatureDistributionsDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_CLUSTER_ANALYSIS, FeaturestoreHelper
        .convertClusterAnalysisDTOToJsonObject(statisticsDTO.getClusterAnalysisDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_UPDATE_METADATA, false);
    json.put(Constants.JSON_FEATUREGROUP_UPDATE_STATS, true);
    Response response = null;
    try {
      response = clientWrapper(json, Constants.HOPSWORKS_REST_APPSERVICE_UPDATE_TRAINING_DATASET_RESOURCE,
          HttpMethod.PUT);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupUpdateStatsError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = parseHopsworksErrorResponse(response);
      throw new FeaturegroupUpdateStatsError("Could not update statistics for trainingDataset:" + trainingDataset +
          " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
          + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
    return response;
  }

  /**
   * Creates a new featuregroup from a spark dataframe
   *
   * @param sparkSession        the spark session
   * @param featuregroupDf      the spark dataframe
   * @param featuregroup        the name of the featuregroup
   * @param featurestore        the featurestore of the featuregroup (defaults to the project's featurestore)
   * @param featuregroupVersion the version of the featuregroup (defaults to 1)
   * @param description         a description of the featuregroup
   * @param jobName             name of the job to compute the feature group
   * @param dependencies        list of the datasets that this featuregroup depends on (e.g input datasets to the
   *                            feature engineering job)
   * @param primaryKey          the primary key of the new featuregroup, if not specified, the first column in the
   *                            dataframe will be used as primary
   * @param descriptiveStats    a bolean flag whether to compute descriptive statistics (min,max,mean etc) for the
   *                            featuregroup
   * @param featureCorr         a boolean flag whether to compute a feature correlation matrix for the numeric
   *                            columns in the featuregroup
   * @param featureHistograms   a boolean flag whether to compute histograms for the numeric columns in the
   *                            featuregroup
   * @param clusterAnalysis     a boolean flag whether to compute cluster analysis for the numeric columns in the
   *                            featuregroup
   * @param statColumns         a list of columns to compute statistics for (defaults to all columns that are numeric)
   * @param numBins             number of bins to use for computing histograms
   * @param corrMethod          the method to compute feature correlation with (pearson or spearman)
   * @param numClusters         the number of clusters to use for cluster analysis
   * @throws InvalidPrimaryKeyForFeaturegroup InvalidPrimaryKeyForFeaturegroup
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws JAXBException JAXBException
   * @throws FeaturegroupCreationError FeaturegroupCreationError
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   */
  public static void createFeaturegroup(
      SparkSession sparkSession, Dataset<Row> featuregroupDf, String featuregroup, String featurestore,
      int featuregroupVersion, String description, String jobName,
      List<String> dependencies, String primaryKey, Boolean descriptiveStats, Boolean featureCorr,
      Boolean featureHistograms, Boolean clusterAnalysis, List<String> statColumns, Integer numBins,
      String corrMethod, Integer numClusters)
      throws InvalidPrimaryKeyForFeaturegroup,
      CredentialsNotFoundException, DataframeIsEmpty, JAXBException, FeaturegroupCreationError,
      SparkDataTypeNotRecognizedError {
    FeaturestoreHelper.validateMetadata(featuregroup, featuregroupDf.dtypes(), dependencies, description);
    if (featurestore == null)
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    if (sparkSession == null)
      sparkSession = findSpark();
    if (primaryKey == null) {
      primaryKey = FeaturestoreHelper.getDefaultPrimaryKey(featuregroupDf);
    }
    if (corrMethod == null) {
      corrMethod = Constants.CORRELATION_ANALYSIS_DEFAULT_METHOD;
    }
    if (numBins == null) {
      numBins = 20;
    }
    if (numClusters == null) {
      numClusters = 5;
    }
    FeaturestoreHelper.validatePrimaryKey(featuregroupDf, primaryKey);
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(featuregroup, sparkSession, featuregroupDf,
        featurestore, featuregroupVersion,
        descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns, numBins, numClusters,
        corrMethod);
    List<FeatureDTO> featuresSchema = FeaturestoreHelper.parseSparkFeaturesSchema(featuregroupDf.schema(), primaryKey);
    createFeaturegroupRest(featurestore, featuregroup, featuregroupVersion, description,
        jobName, dependencies, featuresSchema, statisticsDTO);
    FeaturestoreHelper.insertIntoFeaturegroup(featuregroupDf, sparkSession, featuregroup,
        featurestore, featuregroupVersion);
  }

  /**
   * Creates a new training dataset from a spark dataframe, saves metadata about the training dataset to the database
   * and saves the materialized dataset on hdfs
   *
   * @param sparkSession           the spark session
   * @param trainingDatasetDf      the spark dataframe to create the training dataset from
   * @param trainingDataset        the name of the training dataset
   * @param featurestore           the featurestore that the training dataset is linked to
   * @param trainingDatasetVersion the version of the training dataset (defaults to 1)
   * @param description            a description of the training dataset
   * @param jobName                the name of the job to compute the training dataset
   * @param dataFormat             the format of the materialized training dataset
   * @param dependencies           list of the datasets that this training dataset depends on
   *                               (e.g input datasets to the feature engineering job)
   * @param descriptiveStats       a bolean flag whether to compute descriptive statistics
   *                               (min,max,mean etc) for the featuregroup
   * @param featureCorr            a boolean flag whether to compute a feature correlation matrix for the
   *                               numeric columns in the featuregroup
   * @param featureHistograms      a boolean flag whether to compute histograms for the numeric columns in the
   *                               featuregroup
   * @param clusterAnalysis        a boolean flag whether to compute cluster analysis for the numeric columns in the
   *                               featuregroup
   * @param statColumns            a list of columns to compute statistics for (defaults to all columns
   *                               that are numeric)
   * @param numBins                number of bins to use for computing histograms
   * @param corrMethod             the method to compute feature correlation with (pearson or spearman)
   * @param numClusters            number of clusters to use for cluster analysis
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws JAXBException JAXBException
   * @throws TrainingDatasetCreationError TrainingDatasetCreationError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   */
  public static void createTrainingDataset(
      SparkSession sparkSession, Dataset<Row> trainingDatasetDf, String trainingDataset, String featurestore,
      int trainingDatasetVersion, String description, String jobName, String dataFormat,
      List<String> dependencies, Boolean descriptiveStats, Boolean featureCorr,
      Boolean featureHistograms, Boolean clusterAnalysis, List<String> statColumns, Integer numBins,
      String corrMethod, Integer numClusters)
      throws CredentialsNotFoundException, DataframeIsEmpty, JAXBException, TrainingDatasetCreationError,
      TrainingDatasetFormatNotSupportedError, SparkDataTypeNotRecognizedError {
    FeaturestoreHelper.validateMetadata(trainingDataset, trainingDatasetDf.dtypes(), dependencies, description);
    if (featurestore == null)
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    if (sparkSession == null)
      sparkSession = findSpark();
    if (dataFormat == null) {
      dataFormat = Constants.TRAINING_DATASET_TFRECORDS_FORMAT;
    }
    if (corrMethod == null) {
      corrMethod = Constants.CORRELATION_ANALYSIS_DEFAULT_METHOD;
    }
    if (numBins == null) {
      numBins = 20;
    }
    if (numClusters == null) {
      numClusters = 5;
    }
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(trainingDataset, sparkSession,
        trainingDatasetDf, featurestore, trainingDatasetVersion,
        descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns, numBins, numClusters,
        corrMethod);
    List<FeatureDTO> featuresSchema = FeaturestoreHelper.parseSparkFeaturesSchema(trainingDatasetDf.schema(),
        null);
    Response response = createTrainingDatasetRest(featurestore, trainingDataset, trainingDatasetVersion, description,
        jobName, dataFormat, dependencies, featuresSchema, statisticsDTO);
    String jsonStrResponse = response.readEntity(String.class);
    JSONObject jsonObjResponse = new JSONObject(jsonStrResponse);
    TrainingDatasetDTO trainingDatasetDTO = FeaturestoreHelper.parseTrainingDatasetJson(jsonObjResponse);
    String hdfsPath = trainingDatasetDTO.getHdfsStorePath() + Constants.SLASH_DELIMITER + trainingDataset;
    FeaturestoreHelper.writeTrainingDatasetHdfs(
        sparkSession, trainingDatasetDf, hdfsPath, dataFormat, Constants.SPARK_OVERWRITE_MODE);
    if (dataFormat == Constants.TRAINING_DATASET_TFRECORDS_FORMAT) {
      try {
        JSONObject tfRecordSchemaJson = FeaturestoreHelper.getDataframeTfRecordSchemaJson(trainingDatasetDf);
        FeaturestoreHelper.writeTfRecordSchemaJson(trainingDatasetDTO.getHdfsStorePath()
                + Constants.SLASH_DELIMITER + Constants.TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME,
            tfRecordSchemaJson.toString());
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Could not save tf record schema json to HDFS for training dataset: "
            + trainingDataset, e);
      }
    }
  }

  /**
   * Gets the latest version of a feature group in the feature store, returns 0 if no version exists
   *
   * @param featuregroupName the name of the featuregroup to get the latest version of
   * @param featurestore     the featurestore where the featuregroup resides
   * @return the latest version of the feature group
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws  FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static int getLatestFeaturegroupVersion(
      String featuregroupName, String featurestore)
      throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    if (featurestore == null)
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    FeaturegroupsAndTrainingDatasetsDTO featuregroupsAndTrainingDatasetsDTO = getFeaturestoreMetadata(featurestore);
    List<FeaturegroupDTO> featuregroupDTOList = featuregroupsAndTrainingDatasetsDTO.getFeaturegroups();
    return FeaturestoreHelper.getLatestFeaturegroupVersion(featuregroupDTOList, featuregroupName);
  }

  /**
   * Gets the latest version of a training dataset in the feature store, returns 0 if no version exists
   *
   * @param trainingDatasetName the name of the trainingDataset to get the latest version of
   * @param featurestore        the featurestore where the training dataset resides
   * @return the latest version of the training dataset
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static int getLatestTrainingDatasetVersion(
      String trainingDatasetName, String featurestore)
      throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    if (featurestore == null)
      featurestore = FeaturestoreHelper.getProjectFeaturestore();
    FeaturegroupsAndTrainingDatasetsDTO featuregroupsAndTrainingDatasetsDTO = getFeaturestoreMetadata(featurestore);
    List<TrainingDatasetDTO> trainingDatasetDTOS = featuregroupsAndTrainingDatasetsDTO.getTrainingDatasets();
    return FeaturestoreHelper.getLatestTrainingDatasetVersion(trainingDatasetDTOS, trainingDatasetName);
  }

  /**
   * Finds the spark session dynamically if it is not provided by the user-request
   *
   * @return spark session
   */
  public static SparkSession findSpark() {
    return SparkSession.builder().getOrCreate();
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
