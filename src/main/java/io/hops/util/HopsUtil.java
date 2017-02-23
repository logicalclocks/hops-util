package io.hops.util;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import io.hops.util.flink.FlinkConsumer;
import io.hops.util.flink.FlinkProducer;
import io.hops.util.spark.SparkProducer;
import io.hops.util.spark.SparkConsumer;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.Cookie;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONObject;

/**
 * Hops utility class to be used by applications that want to communicate
 * with Kafka.
 * <p>
 */
public class HopsUtil {

  private static final Logger LOG = Logger.getLogger(HopsUtil.class.getName());

  public static final String KAFKA_FLINK_PARAMS = "kafka_params";
  public static final String KAFKA_SESSIONID_ENV_VAR = "hopsworks.sessionid";
  public static final String KAFKA_PROJECTID_ENV_VAR = "hopsworks.projectid";
  public static final String KAFKA_PROJECTNAME_ENV_VAR = "hopsworks.projectname";
  public static final String JOBNAME_ENV_VAR = "hopsworks.jobname";
  public static final String KAFKA_BROKERADDR_ENV_VAR
      = "hopsworks.kafka.brokeraddress";
  public static final String KAFKA_K_CERTIFICATE_ENV_VAR = "kafka_k_certificate";
  public static final String KAFKA_T_CERTIFICATE_ENV_VAR = "kafka_t_certificate";
  public static final String KAFKA_RESTENDPOINT = "hopsworks.kafka.restendpoint";
  public static final String KAFKA_TOPICS_ENV_VAR = "hopsworks.kafka.job.topics";
  public static final String KAFKA_CONSUMER_GROUPS
      = "hopsworks.kafka.consumergroups";
  public static final String KEYSTORE_PWD_ENV_VAR
      = "hopsworks.keystore.password";
  public static final String TRUSTSTORE_PWD_ENV_VAR
      = "hopsworks.truststore.password";
  public static final String ELASTIC_ENDPOINT_ENV_VAR = "hopsworks.elastic.endpoint";
  public static final String HOPSWORKS_REST_RESOURCE = "hopsworks-api/api/project";

  private static HopsUtil instance = null;
  private static boolean isSetup;

  private static String jSessionId;
  private static Integer projectId;
  private static String projectName;
  private static String jobName;
  private static String brokerEndpoint;
  private static String restEndpoint;
  private static String keyStore;
  private static String trustStore;
  private static String keystorePwd;
  private static String truststorePwd;
  private static List<String> topics;
  private static List<String> consumerGroups;
  private static String elasticEndPoint;

  static {
    setup();
  }

  private HopsUtil() {

  }

  /**
   * Setup the Kafka instance.
   * <p>
   */
  public static synchronized void setup() {
    Properties sysProps = System.getProperties();
    System.out.println("sysProps:" + sysProps);
    //If the sysProps are properly set, it is a Spark job. Flink jobs must call the setup method.
    if (sysProps.containsKey(KAFKA_SESSIONID_ENV_VAR)) {
      System.out.println("sysProps contains:" + KAFKA_SESSIONID_ENV_VAR);
      //validate arguments first
      jSessionId = sysProps.getProperty(KAFKA_SESSIONID_ENV_VAR);
      projectId = Integer.parseInt(sysProps.getProperty(KAFKA_PROJECTID_ENV_VAR));
      projectName = sysProps.getProperty(KAFKA_PROJECTNAME_ENV_VAR);
      jobName = sysProps.getProperty(JOBNAME_ENV_VAR);
      brokerEndpoint = sysProps.getProperty(KAFKA_BROKERADDR_ENV_VAR);
      restEndpoint = sysProps.getProperty(KAFKA_RESTENDPOINT) + File.separator + HOPSWORKS_REST_RESOURCE;
      keyStore = KAFKA_K_CERTIFICATE_ENV_VAR;
      trustStore = KAFKA_T_CERTIFICATE_ENV_VAR;
      keystorePwd = sysProps.getProperty(KEYSTORE_PWD_ENV_VAR);
      truststorePwd = sysProps.getProperty(TRUSTSTORE_PWD_ENV_VAR);
      elasticEndPoint = sysProps.getProperty(ELASTIC_ENDPOINT_ENV_VAR);
      //Spark Kafka topics
      topics = Arrays.asList(sysProps.getProperty(KAFKA_TOPICS_ENV_VAR).split(File.pathSeparator));
      if (sysProps.containsKey(KAFKA_CONSUMER_GROUPS)) {
        consumerGroups = Arrays.asList(sysProps.getProperty(KAFKA_CONSUMER_GROUPS).split(File.pathSeparator));
      }
    }
  }

  /**
   * Setup the Kafka instance.
   * Endpoint is where the REST API listens for requests. I.e.
   * http://localhost:8080/. Similarly set domain to "localhost"
   * <p>
   * @param endpoint
   * @param domain
   * @return
   */
  public synchronized HopsUtil setup(String endpoint, String domain) {
    Properties sysProps = System.getProperties();

    //validate arguments first
    this.jSessionId = sysProps.getProperty(KAFKA_SESSIONID_ENV_VAR);
    this.projectId = Integer.parseInt(sysProps.getProperty(
        KAFKA_PROJECTID_ENV_VAR));
    this.projectName = sysProps.getProperty(KAFKA_PROJECTNAME_ENV_VAR);
    this.brokerEndpoint = sysProps.getProperty(KAFKA_BROKERADDR_ENV_VAR);
    this.restEndpoint = endpoint + File.separator + HOPSWORKS_REST_RESOURCE;
    this.keyStore = KAFKA_K_CERTIFICATE_ENV_VAR;
    this.trustStore = KAFKA_T_CERTIFICATE_ENV_VAR;
    this.keystorePwd = sysProps.getProperty(KEYSTORE_PWD_ENV_VAR);
    this.truststorePwd = sysProps.getProperty(TRUSTSTORE_PWD_ENV_VAR);
    isSetup = true;
    return this;
  }

  /**
   * Setup the Kafka instance.
   * Endpoint is where the REST API listens for requests. I.e.
   * http://localhost:8080/. Similarly set domain to "localhost"
   * KeyStore and TrustStore locations should on the local machine.
   *
   * @param topicName
   * @param restEndpoint
   * @param keyStore
   * @param trustStore
   * @param domain
   * @return
   */
  public synchronized HopsUtil setup(String topicName, String restEndpoint,
      String keyStore,
      String trustStore, String domain) {
    Properties sysProps = System.getProperties();

    //validate arguments first
    this.jSessionId = sysProps.getProperty(KAFKA_SESSIONID_ENV_VAR);
    this.projectId = Integer.parseInt(sysProps.getProperty(
        KAFKA_PROJECTID_ENV_VAR));
    this.projectName = sysProps.getProperty(KAFKA_PROJECTNAME_ENV_VAR);
    this.brokerEndpoint = sysProps.getProperty(KAFKA_BROKERADDR_ENV_VAR);
    this.restEndpoint = restEndpoint + File.separator + HOPSWORKS_REST_RESOURCE;
    this.keyStore = keyStore;
    this.trustStore = trustStore;
    this.keystorePwd = sysProps.getProperty(KEYSTORE_PWD_ENV_VAR);
    this.truststorePwd = sysProps.getProperty(TRUSTSTORE_PWD_ENV_VAR);
    isSetup = true;
    return this;
  }

  /**
   * Setup the Kafka instance.Endpoint is where the REST API listens for
   * requests. I.e.
   * http://localhost:8080/. Similarly set domain to "localhost"
   * KeyStore and TrustStore locations should on the local machine.
   *
   * @param jSessionId
   * @param projectId
   * @param topicName
   * @param brokerEndpoint
   * @param restEndpoint
   * @param keyStore
   * @param trustStore
   * @param keystorePwd
   * @param truststorePwd
   * @return
   */
  public synchronized HopsUtil setup(String jSessionId, int projectId,
      String topicName, String brokerEndpoint, String restEndpoint,
      String keyStore, String trustStore, String keystorePwd,
      String truststorePwd) {
    this.jSessionId = jSessionId;
    this.projectId = projectId;
    this.brokerEndpoint = brokerEndpoint;
    this.restEndpoint = restEndpoint + File.separator + HOPSWORKS_REST_RESOURCE;
    this.keyStore = keyStore;
    this.trustStore = trustStore;
    this.keystorePwd = keystorePwd;
    this.trustStore = truststorePwd;
    isSetup = true;
    return this;
  }

  /**
   * Setup the Kafka instance.Endpoint is where the REST API listens for
   * requests. I.e.
   * http://localhost:8080/. Similarly set domain to "localhost"
   * KeyStore and TrustStore locations should on the local machine.
   *
   * @param jSessionId
   * @param projectId
   * @param topics
   * @param consumerGroups
   * @param brokerEndpoint
   * @param restEndpoint
   * @param keyStore
   * @param trustStore
   * @param keystorePwd
   * @param truststorePwd
   * @return
   */
  public synchronized HopsUtil setup(String jSessionId, int projectId,
      String topics, String consumerGroups, String brokerEndpoint,
      String restEndpoint,
      String keyStore, String trustStore, String keystorePwd,
      String truststorePwd) {
    this.jSessionId = jSessionId;
    this.projectId = projectId;
    this.brokerEndpoint = brokerEndpoint;
    this.restEndpoint = restEndpoint + File.separator + HOPSWORKS_REST_RESOURCE;
    this.topics = Arrays.asList(topics.split(File.pathSeparator));
    this.consumerGroups = Arrays.
        asList(consumerGroups.split(File.pathSeparator));
    this.keyStore = keyStore;
    this.trustStore = trustStore;
    this.keystorePwd = keystorePwd;
    this.truststorePwd = truststorePwd;
    isSetup = true;
    return this;
  }

  /**
   * Setup the Kafka instance by using a Map of parameters.
   *
   * @param params
   * @return
   */
  public static synchronized void setup(Map<String, String> params) {
    jSessionId = params.get(KAFKA_SESSIONID_ENV_VAR);
    projectId = Integer.parseInt(params.get(
        HopsUtil.KAFKA_PROJECTID_ENV_VAR));
    brokerEndpoint = params.get(HopsUtil.KAFKA_BROKERADDR_ENV_VAR);
    restEndpoint = params.get(HopsUtil.KAFKA_RESTENDPOINT)
        + File.separator + HOPSWORKS_REST_RESOURCE;
    topics = Arrays.asList(params.get(HopsUtil.KAFKA_TOPICS_ENV_VAR).split(
        File.pathSeparator));
    if (params.containsKey(KAFKA_CONSUMER_GROUPS)) {
      consumerGroups = Arrays.asList(params.get(
          HopsUtil.KAFKA_CONSUMER_GROUPS).split(
              File.pathSeparator));
    }
    keyStore = params.get(KAFKA_K_CERTIFICATE_ENV_VAR);
    trustStore = params.get(KAFKA_T_CERTIFICATE_ENV_VAR);
    keystorePwd = params.get(KEYSTORE_PWD_ENV_VAR);
    truststorePwd = params.get(TRUSTSTORE_PWD_ENV_VAR);
    isSetup = true;
  }

  /**
   * Instantiates and provides a singleton HopsUtil. Flink application must
   * then call the setup() method.
   *
   * @return
   */
//  public static HopsUtil getInstance() {
//    if (instance == null) {
//      instance = new HopsUtil();
//      if (!isSetup && System.getProperties().containsKey(KAFKA_SESSIONID_ENV_VAR)) {
//        instance.setup();
//      }
//    }
//    return instance;
//  }
  public static KafkaProperties getKafkaProperties() {
    return new KafkaProperties();
  }

  public static HopsConsumer getHopsConsumer(String topic) throws
      SchemaNotFoundException {
    return new HopsConsumer(topic);
  }

  public static HopsProducer getHopsProducer(String topic) throws
      SchemaNotFoundException {
    return new HopsProducer(topic);
  }

  public static FlinkConsumer getFlinkConsumer(String topic) {
    return getFlinkConsumer(topic, new AvroDeserializer(topic));
  }

  public static FlinkConsumer getFlinkConsumer(String topic,
      DeserializationSchema deserializationSchema) {
    return new FlinkConsumer(topic, deserializationSchema,
        getKafkaProperties().getConsumerConfig());
  }

  public static FlinkProducer getFlinkProducer(String topic) {
    return getFlinkProducer(topic, new AvroDeserializer(topic));
  }

  /**
   *
   * @param topic
   * @param serializationSchema
   * @return
   */
  public static FlinkProducer getFlinkProducer(String topic,
      SerializationSchema serializationSchema) {
    return new FlinkProducer(topic, serializationSchema,
        getKafkaProperties().defaultProps());
  }

  /**
   *
   * @return
   * @throws SchemaNotFoundException
   * @throws TopicNotFoundException
   */
  public static SparkProducer getSparkProducer() throws
      SchemaNotFoundException, TopicNotFoundException {
    if (HopsUtil.getTopics() != null && HopsUtil.getTopics().size() == 1) {
      return new SparkProducer(HopsUtil.getTopics().get(0));
    } else {
      throw new TopicNotFoundException(
          "No topic was found for this spark producer");
    }
  }

  /**
   *
   * @param topic
   * @return
   * @throws SchemaNotFoundException
   */
  public static SparkProducer getSparkProducer(String topic) throws
      SchemaNotFoundException {
    return new SparkProducer(topic);
  }

  /**
   *
   * @param jsc
   * @return
   * @throws TopicNotFoundException
   */
  public static SparkConsumer getSparkConsumer(JavaStreamingContext jsc) throws
      TopicNotFoundException {
    if (topics != null && !topics.isEmpty()) {
      return new SparkConsumer(jsc, topics);
    } else {
      throw new TopicNotFoundException(
          "No topic was found for this spark consumer");
    }
  }

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
   *
   * @param jsc
   * @param topics
   * @return
   */
  public static SparkConsumer getSparkConsumer(JavaStreamingContext jsc,
      Collection<String> topics) {
    return new SparkConsumer(jsc, topics);
  }

  /**
   *
   * @param jsc
   * @param topics
   * @param userProps
   * @return
   */
  public static SparkConsumer getSparkConsumer(JavaStreamingContext jsc,
      Collection<String> topics, Properties userProps) {
    return new SparkConsumer(jsc, topics, userProps);
  }

  /**
   *
   * @param topic
   * @return
   */
  public AvroDeserializer getHopsAvroSchema(String topic) {
    return new AvroDeserializer(topic);
  }

  public static String getSchema(String topicName) throws SchemaNotFoundException {
    return getSchema(topicName, Integer.MIN_VALUE);
  }

  /**
   * Get Avro Schema directly using topics extracted from Spark HopsWorks Jobs
   * UI.
   *
   * @return
   */
  public static Map<String, Schema> getSchemas() {
    Map<String, Schema> schemas = new HashMap<>();
    for (String topic : HopsUtil.getTopics()) {
      try {
        Schema.Parser parser = new Schema.Parser();
        schemas.put(topic, parser.parse(getSchema(topic)));
      } catch (SchemaNotFoundException ex) {
        LOG.log(Level.SEVERE, "Could not get schema for topic", ex);
      }
    }
    return schemas;
  }

  /**
   * Get record Injections for Avro messages, using topics extracted from Spark
   * HopsWorks Jobs UI.
   *
   * @return
   */
  public static Map<String, Injection<GenericRecord, byte[]>> getRecordInjections() {
    Map<String, Schema> schemas = HopsUtil.getSchemas();
    Map<String, Injection<GenericRecord, byte[]>> recordInjections
        = new HashMap<>();
    for (String topic : HopsUtil.getTopics()) {
      recordInjections.
          put(topic, GenericAvroCodecs.toBinary(schemas.get(topic)));

    }
    return recordInjections;
  }

  public static String getSchema(String topicName, int versionId) throws
      SchemaNotFoundException {
    System.out.println("kafka.hopsutil.topicName:" + topicName);
    String uri = HopsUtil.getRestEndpoint() + "/" + HopsUtil.getProjectId() + "/kafka/schema/" + topicName;
    if (versionId > 0) {
      uri += "/" + versionId;
    }
    System.out.println("kafka.hopsutil.uri:" + uri);

    ClientConfig config = new DefaultClientConfig();
    Client client = Client.create(config);
    WebResource service = client.resource(uri);
    Cookie cookie = new Cookie("SESSIONID", HopsUtil.getjSessionId());
    final ClientResponse blogResponse = service.cookie(cookie).get(
        ClientResponse.class);
    final String blog = blogResponse.getEntity(String.class);
    //Extract fields from json
    JSONObject json = new JSONObject(blog);
    String schema = json.getString("contents");

    return schema;

  }

  public static Logger getLogger() {
    return LOG;
  }

  public static String getBrokerEndpoint() {
    return brokerEndpoint;
  }

  public static String getjSessionId() {
    return jSessionId;
  }

  public static Integer getProjectId() {
    return projectId;
  }

  public static String getRestEndpoint() {
    return restEndpoint;
  }

  public static String getKeyStore() {
    return keyStore;
  }

  public static String getTrustStore() {
    return trustStore;
  }

  public static String getKeystorePwd() {
    return keystorePwd;
  }

  public static String getTruststorePwd() {
    return truststorePwd;
  }

  public static List<String> getTopics() {
    return topics;
  }

  public static List<String> getConsumerGroups() {
    return consumerGroups;
  }

  public static String getProjectName() {
    return projectName;
  }

  public static String getElasticEndPoint() {
    return elasticEndPoint;
  }

  public static String getJobName() {
    return jobName;
  }
  
  /**
   * Utility method for Flink applications that need to parse Flink system
   * variables.
   *
   * @param propsStr
   * @return
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

}
