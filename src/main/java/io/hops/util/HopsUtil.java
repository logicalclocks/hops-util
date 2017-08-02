package io.hops.util;

import com.google.common.io.ByteStreams;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import io.hops.util.flink.FlinkConsumer;
import io.hops.util.flink.FlinkProducer;
import io.hops.util.spark.SparkConsumer;
import io.hops.util.spark.SparkProducer;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;
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
 * Hops utility class to be used by applications that want to communicate
 * with Kafka.
 * <p>
 */
public class HopsUtil {

  private static final Logger LOG = Logger.getLogger(HopsUtil.class.getName());

  public static final String KAFKA_FLINK_PARAMS = "kafka_params";
  public static final String KAFKA_PROJECTID_ENV_VAR = "hopsworks.projectid";
  public static final String KAFKA_PROJECTNAME_ENV_VAR = "hopsworks.projectname";
  public static final String JOBNAME_ENV_VAR = "hopsworks.job.name";
  public static final String JOBTYPE_ENV_VAR = "hopsworks.job.type";
  public static final String APPID_ENV_VAR = "hopsworks.job.appid";
  public static final String KAFKA_BROKERADDR_ENV_VAR = "hopsworks.kafka.brokeraddress";
  public static final String K_CERTIFICATE_ENV_VAR = "k_certificate";
  public static final String T_CERTIFICATE_ENV_VAR = "t_certificate";
  public static final String HOPSWORKS_RESTENDPOINT = "hopsworks.restendpoint";
  public static final String KAFKA_TOPICS_ENV_VAR = "hopsworks.kafka.job.topics";
  public static final String KAFKA_CONSUMER_GROUPS = "hopsworks.kafka.consumergroups";
  public static final String KEYSTORE_ENV_VAR = "hopsworks.keystore";
  public static final String TRUSTSTORE_ENV_VAR = "hopsworks.truststore";
  public static final String KEYSTORE_VAL_ENV_VAR = "keyPw";
  public static final String TRUSTSTORE_VAL_ENV_VAR = "trustPw";
  public static final String ELASTIC_ENDPOINT_ENV_VAR = "hopsworks.elastic.endpoint";
  public static final String HOPSWORKS_REST_RESOURCE = "hopsworks-api/api";
  public static final String HOPSWORKS_REST_APPSERVICE = "appservice";
  public static final String HOPSWORKS_REST_CERTSERVICE = "certs";
  public static final String SESSIONID_ENV_VAR = "hopsworks.sessionid";

  private static Integer projectId;
  private static String projectName;
  private static String jobName;
  private static String appId;
  private static String jobType;
  private static String brokerEndpoint;
  private static String restEndpoint;
  private static String keyStore;
  private static String trustStore;
  private static String keystorePwd;
  private static String truststorePwd;
  private static List<String> topics;
  private static List<String> consumerGroups;
  private static String elasticEndPoint;
  private static SparkInfo sparkInfo;
  private static String sessionId;

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
    //If the sysProps are properly set, it is a Spark job. Flink jobs must call the setup method.
    if (sysProps.containsKey(JOBTYPE_ENV_VAR) && sysProps.getProperty(JOBTYPE_ENV_VAR).equalsIgnoreCase("spark")) {
      try {
        sessionId = sysProps.getProperty(SESSIONID_ENV_VAR);
        restEndpoint = sysProps.getProperty(HOPSWORKS_RESTENDPOINT);
        keyStore = K_CERTIFICATE_ENV_VAR;
        trustStore = T_CERTIFICATE_ENV_VAR;
        //Get keystore and truststore passwords from Hopsworks
        projectId = Integer.parseInt(sysProps.getProperty(KAFKA_PROJECTID_ENV_VAR));
        JSONObject pw = getCertPw();
        keystorePwd = pw.getString(KEYSTORE_VAL_ENV_VAR);
        truststorePwd = pw.getString(TRUSTSTORE_VAL_ENV_VAR);
        projectName = sysProps.getProperty(KAFKA_PROJECTNAME_ENV_VAR);
        jobName = sysProps.getProperty(JOBNAME_ENV_VAR);
        appId = sysProps.getProperty(APPID_ENV_VAR);
        jobType = sysProps.getProperty(JOBTYPE_ENV_VAR);
        brokerEndpoint = sysProps.getProperty(KAFKA_BROKERADDR_ENV_VAR);
        elasticEndPoint = sysProps.getProperty(ELASTIC_ENDPOINT_ENV_VAR);
        //Spark Kafka topics
        topics = Arrays.asList(sysProps.getProperty(KAFKA_TOPICS_ENV_VAR).split(File.pathSeparator));
        if (sysProps.containsKey(KAFKA_CONSUMER_GROUPS)) {
          consumerGroups = Arrays.asList(sysProps.getProperty(KAFKA_CONSUMER_GROUPS).split(File.pathSeparator));
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
   * @param endpoint
   * @param domain
   * @return
   */
  public synchronized HopsUtil setup(String endpoint, String domain) {
    Properties sysProps = System.getProperties();

    //validate arguments first
    this.projectId = Integer.parseInt(sysProps.getProperty(
        KAFKA_PROJECTID_ENV_VAR));
    this.projectName = sysProps.getProperty(KAFKA_PROJECTNAME_ENV_VAR);
    this.brokerEndpoint = sysProps.getProperty(KAFKA_BROKERADDR_ENV_VAR);
    this.restEndpoint = endpoint + File.separator + HOPSWORKS_REST_RESOURCE;
    this.keyStore = K_CERTIFICATE_ENV_VAR;
    this.trustStore = T_CERTIFICATE_ENV_VAR;
//    this.keystorePwd = sysProps.getProperty(KEYSTORE_PWD_ENV_VAR);
//    this.truststorePwd = sysProps.getProperty(TRUSTSTORE_PWD_ENV_VAR);
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
    this.projectId = Integer.parseInt(sysProps.getProperty(
        KAFKA_PROJECTID_ENV_VAR));
    this.projectName = sysProps.getProperty(KAFKA_PROJECTNAME_ENV_VAR);
    this.brokerEndpoint = sysProps.getProperty(KAFKA_BROKERADDR_ENV_VAR);
    this.restEndpoint = restEndpoint + File.separator + HOPSWORKS_REST_RESOURCE;
    this.keyStore = keyStore;
    this.trustStore = trustStore;
//    this.keystorePwd = sysProps.getProperty(KEYSTORE_PWD_ENV_VAR);
//    this.truststorePwd = sysProps.getProperty(TRUSTSTORE_PWD_ENV_VAR);
    return this;
  }

  /**
   * Setup the Kafka instance.Endpoint is where the REST API listens for
   * requests. I.e.
   * http://localhost:8080/. Similarly set domain to "localhost"
   * KeyStore and TrustStore locations should on the local machine.
   *
   * @param pId
   * @param topicN
   * @param brokerE
   * @param restE
   * @param keySt
   * @param trustSt
   * @param keystPwd
   * @param truststPwd
   * @return
   */
  public static synchronized void setup(int pId,
      String topicN, String brokerE, String restE,
      String keySt, String trustSt, String keystPwd,
      String truststPwd) {
    brokerEndpoint = brokerE;
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
  public synchronized HopsUtil setup(int projectId,
      String topics, String consumerGroups, String brokerEndpoint,
      String restEndpoint,
      String keyStore, String trustStore, String keystorePwd,
      String truststorePwd) {
    this.projectId = projectId;
    this.brokerEndpoint = brokerEndpoint;
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
   * @param params
   * @return
   */
  public static synchronized void setup(Map<String, String> params) {
    projectId = Integer.parseInt(params.get(
        HopsUtil.KAFKA_PROJECTID_ENV_VAR));
    brokerEndpoint = params.get(HopsUtil.KAFKA_BROKERADDR_ENV_VAR);
    restEndpoint = params.get(HopsUtil.HOPSWORKS_RESTENDPOINT);
    topics = Arrays.asList(params.get(HopsUtil.KAFKA_TOPICS_ENV_VAR).split(
        File.pathSeparator));
    if (params.containsKey(KAFKA_CONSUMER_GROUPS)) {
      consumerGroups = Arrays.asList(params.get(
          HopsUtil.KAFKA_CONSUMER_GROUPS).split(
              File.pathSeparator));
    }
    keyStore = params.get(K_CERTIFICATE_ENV_VAR);
    trustStore = params.get(T_CERTIFICATE_ENV_VAR);
//    keystorePwd = params.get(KEYSTORE_PWD_ENV_VAR);
//    truststorePwd = params.get(TRUSTSTORE_PWD_ENV_VAR);
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
      SchemaNotFoundException, CredentialsNotFoundException {
    return new HopsConsumer(topic);
  }

  public static HopsProducer getHopsProducer(String topic) throws
      SchemaNotFoundException, CredentialsNotFoundException {
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
      SchemaNotFoundException, TopicNotFoundException, CredentialsNotFoundException {
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
  public static SparkProducer getSparkProducer(String topic) throws SchemaNotFoundException,
      CredentialsNotFoundException {
    return new SparkProducer(topic);
  }

  /**
   *
   * @return
   */
  public static SparkConsumer getSparkConsumer() {
    return new SparkConsumer();
  }

  /**
   *
   * @param topics
   * @return
   * @throws TopicNotFoundException
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
   *
   * @param jsc
   * @return
   * @throws TopicNotFoundException
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
   *
   * @param jsc
   * @param userProps
   * @return
   * @throws TopicNotFoundException
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
   *
   * @param jsc
   * @param topics
   * @return
   */
  public static SparkConsumer getSparkConsumer(JavaStreamingContext jsc, Collection<String> topics) {
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

  public static String getSchema(String topicName) throws SchemaNotFoundException, CredentialsNotFoundException {
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
      } catch (SchemaNotFoundException | CredentialsNotFoundException ex) {
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
    if (schemas.isEmpty()) {
      return null;
    }
    Map<String, Injection<GenericRecord, byte[]>> recordInjections
        = new HashMap<>();
    for (String topic : HopsUtil.getTopics()) {
      recordInjections.
          put(topic, GenericAvroCodecs.toBinary(schemas.get(topic)));

    }
    return recordInjections;
  }

  public static String getSchema(String topicName, int versionId) throws SchemaNotFoundException,
      CredentialsNotFoundException {

    JSONObject json = new JSONObject();
    json.append("keyStorePwd", keystorePwd);
    try {
      json.append("keyStore", keystoreEncode());
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, null, ex);
      throw new CredentialsNotFoundException("Could not initialize HopsUtil properties.");
    }
    json.append("topicName", topicName);
    if (versionId > 0) {
      json.append("version", versionId);
    }
    String uri = HopsUtil.getRestEndpoint() + "/" + HOPSWORKS_REST_RESOURCE + "/" + HOPSWORKS_REST_APPSERVICE
        + "/schema";
    LOG.log(Level.FINE, "Getting schema for topic:{0} from uri:{1}", new String[]{topicName, uri});

    ClientConfig config = new DefaultClientConfig();
    Client client = Client.create(config);
    WebResource service = client.resource(uri);
    final ClientResponse blogResponse = service.type(
        MediaType.APPLICATION_JSON).post(ClientResponse.class, json.
            toString());
    final String blog = blogResponse.getEntity(String.class);
    //Extract fields from json
    json = new JSONObject(blog);
    String schema = json.getString("contents");
    return schema;
  }

  public static String sendEmail(String dest, String subject, String message) throws
      CredentialsNotFoundException {

    String uri = HopsUtil.getRestEndpoint() + "/" + HOPSWORKS_REST_RESOURCE + "/" + HOPSWORKS_REST_APPSERVICE + "/mail";

    ClientConfig config = new DefaultClientConfig();
    Client client = Client.create(config);
    WebResource service = client.resource(uri);
    JSONObject json = new JSONObject();
    json.append("dest", dest);
    json.append("subject", subject);
    json.append("message", message);
    json.append("keyStorePwd", keystorePwd);
    try {
      json.append("keyStore", keystoreEncode());
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, null, ex);
      throw new CredentialsNotFoundException("Could not initialize HopsUtil properties.");
    }
    ClientResponse blogResponse = service.type(MediaType.APPLICATION_JSON).post(ClientResponse.class, json.toString());
    final String response = blogResponse.getEntity(String.class);

    return response;

  }

  private static JSONObject getCertPw() throws CredentialsNotFoundException {

    try {
      String uri = HopsUtil.getRestEndpoint() + "/" + HOPSWORKS_REST_RESOURCE + "/project/" + projectId + "/"
          + HOPSWORKS_REST_CERTSERVICE
          + "/certpw";
      ClientConfig config = new DefaultClientConfig();
      Client client = Client.create(config);
      WebResource service = client.resource(uri);
      Cookie cookie = new Cookie("SESSION", sessionId);

      final ClientResponse blogResponse = service.queryParam("keyStore", keystoreEncode())
          .type(MediaType.TEXT_PLAIN).cookie(cookie)
          .get(ClientResponse.class);
      final String response = blogResponse.getEntity(String.class);
      JSONObject json = new JSONObject(response);
      return json;
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, null, ex);
      throw new CredentialsNotFoundException("Could not initialize HopsUtil properties.");
    }

  }

  private static String keystoreEncode() throws IOException {
    FileInputStream kfin = new FileInputStream(new File(keyStore));
    byte[] kStoreBlob = ByteStreams.toByteArray(kfin);
    return Base64.encodeBase64String(kStoreBlob);
  }

  public static Logger getLogger() {
    return LOG;
  }

  public static String getBrokerEndpoint() {
    return brokerEndpoint;
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

  public static String getTopicsAsCSV() {
    StringBuilder sb = new StringBuilder();
    topics.forEach((topic) -> {
      sb.append(topic).append(",");
    });
    //Delete last comma
    if (sb.charAt(sb.length() - 1) == ',') {
      return sb.substring(0, sb.length() - 1);
    }
    return sb.toString();
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

  public static String getAppId() {
    return appId;
  }

  public static String getJobType() {
    return jobType;
  }

  /**
   * Shutdown gracefully a streaming spark job.
   *
   * @param jssc
   * @throws InterruptedException
   */
  public static void shutdownGracefully(JavaStreamingContext jssc) throws InterruptedException {
    shutdownGracefully(jssc, 3000);
  }

  public static void shutdownGracefully(StreamingQuery query) throws InterruptedException, StreamingQueryException {
    shutdownGracefully(query, 3000);
  }

  /**
   * Shutdown gracefully a streaming spark job.
   *
   * @param jssc
   * @param checkIntervalMillis
   * @throws InterruptedException
   */
  public static void shutdownGracefully(JavaStreamingContext jssc, int checkIntervalMillis) throws InterruptedException {
    boolean isStopped = false;
    while (!isStopped) {
      isStopped = jssc.awaitTerminationOrTimeout(checkIntervalMillis);
      if (!isStopped && sparkInfo.isShutdownRequested()) {
        LOG.info("Marker file has been removed, will attempt to stop gracefully the spark streaming context");
        jssc.stop(true, true);
      }
    }
  }

  /**
   *
   * @param query
   * @param checkIntervalMillis
   * @throws InterruptedException
   * @throws StreamingQueryException
   */
  public static void shutdownGracefully(StreamingQuery query, long checkIntervalMillis) throws InterruptedException,
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
   * @param jsc
   * @throws InterruptedException
   */
  public static void shutdownGracefully(JavaSparkContext jsc) throws InterruptedException {
    while (!sparkInfo.isShutdownRequested()) {
      Thread.sleep(5000);
      LOG.info("Sleeping marker");
    }
    LOG.info("Marker file has been removed, will attempt to stop gracefully the spark context");
    jsc.stop();
    jsc.close();
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
