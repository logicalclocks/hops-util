package io.hops.kafkautil;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import io.hops.kafkautil.flink.FlinkConsumer;
import io.hops.kafkautil.flink.FlinkProducer;
import io.hops.kafkautil.spark.SparkProducer;
import io.hops.kafkautil.spark.SparkConsumer;
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
public class KafkaUtil {

  private static final Logger logger = Logger.getLogger(KafkaUtil.class.
          getName());

  public final String KAFKA_SESSIONID_ENV_VAR = "kafka.sessionid";
  public final String KAFKA_PROJECTID_ENV_VAR = "kafka.projectid";
  public final String KAFKA_BROKERADDR_ENV_VAR = "kafka.brokeraddress";
  public final String KAFKA_K_CERTIFICATE_ENV_VAR = "kafka_k_certificate";
  public final String KAFKA_T_CERTIFICATE_ENV_VAR = "kafka_t_certificate";
  public final String KAFKA_RESTENDPOINT = "kafka.restendpoint";

  private static KafkaUtil instance = null;
  private static boolean isSetup;

  private String jSessionId;
  private Integer projectId;
  private String brokerEndpoint;
  private String restEndpoint;
  private String keyStore;
  private String trustStore;
  private List<String> topics;

  private KafkaUtil() {

  }

  /**
   * Setup the Kafka instance.
   *
   * @return
   */
  public synchronized KafkaUtil setup() {
    Properties sysProps = System.getProperties();
    System.out.println("SysProps:" + sysProps);

    //validate arguments first
    this.jSessionId = sysProps.getProperty("kafka.sessionid");
    this.projectId = Integer.parseInt(sysProps.getProperty("kafka.projectid"));
    this.brokerEndpoint = sysProps.getProperty("kafka.brokeraddress");//"10.0.2.15:9091";
    this.restEndpoint = sysProps.getProperty("kafka.restendpoint")
            + "/hopsworks/api/project";
    this.keyStore = "kafka_k_certificate";//sysProps.getProperty("kafka_k_certificate");
    this.trustStore = "kafka_t_certificate";//"sysProps.getProperty("kafka_t_certificate");
    isSetup = true;
    this.topics = Arrays.asList(sysProps.getProperty(
            "hopsworks.kafka.job.topics").
            split(","));
    return this;
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
  public synchronized KafkaUtil setup(String endpoint, String domain) {
    Properties sysProps = System.getProperties();

    //validate arguments first
    this.jSessionId = sysProps.getProperty("kafka.sessionid");
    this.projectId = Integer.parseInt(sysProps.getProperty("kafka.projectid"));
    this.brokerEndpoint = sysProps.getProperty("kafka.brokeraddress");
    this.restEndpoint = endpoint + "/hopsworks/api/project";
    this.keyStore = "kafka_k_certificate";
    this.trustStore = "kafka_t_certificate";
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
  public synchronized KafkaUtil setup(String topicName, String restEndpoint,
          String keyStore,
          String trustStore, String domain) {
    Properties sysProps = System.getProperties();

    //validate arguments first
    this.jSessionId = sysProps.getProperty("kafka.sessionid");
    this.projectId = Integer.parseInt(sysProps.getProperty("kafka.projectid"));
    this.brokerEndpoint = sysProps.getProperty("kafka.brokeraddress");
    this.restEndpoint = restEndpoint + "/hopsworks/api/project";
    this.keyStore = keyStore;
    this.trustStore = trustStore;
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
   * @param domain
   * @param brokerEndpoint
   * @param restEndpoint
   * @param keyStore
   * @param trustStore
   * @return
   */
  public synchronized KafkaUtil setup(String jSessionId, int projectId,
          String topicName,
          String domain, String brokerEndpoint, String restEndpoint,
          String keyStore, String trustStore) {
    this.jSessionId = jSessionId;
    this.projectId = projectId;
    this.brokerEndpoint = brokerEndpoint;
    this.restEndpoint = restEndpoint + "/hopsworks/api/project";
    this.keyStore = keyStore;
    this.trustStore = trustStore;
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
   * @return
   */
  public synchronized KafkaUtil setup(String jSessionId, int projectId,
          String topicName,
          String brokerEndpoint, String restEndpoint,
          String keyStore, String trustStore) {
    this.jSessionId = jSessionId;
    this.projectId = projectId;
    this.brokerEndpoint = brokerEndpoint;
    this.restEndpoint = restEndpoint + "/hopsworks/api/project";
    this.keyStore = keyStore;
    this.trustStore = trustStore;
    isSetup = true;
    return this;
  }

  /**
   * Instantiates and provides a singleton KafkaUtil. Flink application must
   * then call the setup() method.
   *
   * @return
   */
  public static KafkaUtil getInstance() {
    if (instance == null) {
      instance = new KafkaUtil();
      if (!isSetup && System.getProperties().containsKey("kafka.sessionid")) {
        instance.setup();
      }
    }
    return instance;
  }

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

  public FlinkConsumer getFlinkConsumer(String topic) {
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

  public static FlinkProducer getFlinkProducer(String topic,
          SerializationSchema serializationSchema) {
    return new FlinkProducer(topic, serializationSchema,
            getKafkaProperties().defaultProps());
  }

  public static SparkProducer getSparkProducer(String topic) throws
          SchemaNotFoundException {
    return new SparkProducer(topic);
  }

  public static SparkConsumer getSparkConsumer(JavaStreamingContext jsc,
          Collection<String> topics) {
    return new SparkConsumer(jsc, topics);
  }

  /**
   *
   * @param topic
   * @return
   */
  public AvroDeserializer getHopsAvroSchema(String topic) {
    return new AvroDeserializer(topic);
  }

  public String getSchema(String topicName) throws SchemaNotFoundException {
    return getSchema(topicName, Integer.MIN_VALUE);
  }

  /**
   * Get Avro Schema directly using topics extracted from Spark HopsWorks Jobs
   * UI.
   *
   * @return
   */
  public Map<String, Schema> getSchemas() {

    Map<String, Schema> schemas = new HashMap<>();
    for (String topic : topics) {
      try {
        Schema.Parser parser = new Schema.Parser();
        schemas.put(topic, parser.parse(getSchema(topic)));
      } catch (SchemaNotFoundException ex) {
        logger.log(Level.SEVERE, "Could not get schema for topic", ex);
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
    Map<String, Schema> schemas = KafkaUtil.getInstance().getSchemas();
    Map<String, Injection<GenericRecord, byte[]>> recordInjections
            = new HashMap<>();
    for (String topic : KafkaUtil.getInstance().getTopics()) {
      recordInjections.
              put(topic, GenericAvroCodecs.toBinary(schemas.get(topic)));

    }
    return recordInjections;
  }

  public String getSchema(String topicName, int versionId) throws
          SchemaNotFoundException {
    System.out.println("kafka.hopsutil.topicName:" + topicName);
    String uri = restEndpoint + "/" + projectId + "/kafka/schema/" + topicName;
    if (versionId > 0) {
      uri += "/" + versionId;
    }
    System.out.println("kafka.hopsutil.uri:" + uri);

    ClientConfig config = new DefaultClientConfig();
    Client client = Client.create(config);
    WebResource service = client.resource(uri);
    Cookie cookie = new Cookie("SESSIONID", jSessionId);
    final ClientResponse blogResponse = service.cookie(cookie).get(
            ClientResponse.class);
    final String blog = blogResponse.getEntity(String.class);
    //Extract fields from json
    JSONObject json = new JSONObject(blog);
    String schema = json.getString("contents");

    return schema;

  }

  public String getBrokerEndpoint() {
    return brokerEndpoint;
  }

  public static Logger getLogger() {
    return logger;
  }

  public String getjSessionId() {
    return jSessionId;
  }

  public Integer getProjectId() {
    return projectId;
  }

  public String getRestEndpoint() {
    return restEndpoint;
  }

  public String getKeyStore() {
    return keyStore;
  }

  public String getTrustStore() {
    return trustStore;
  }

  public static List<String> getTopics() {
    return KafkaUtil.getInstance().topics;
  }

  /**
   * Utility method for Flink applications that need to parse Flink system
   * variables.
   *
   * @param propsStr
   * @return
   */
  public Map<String, String> getFlinkKafkaProps(String propsStr) {
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
