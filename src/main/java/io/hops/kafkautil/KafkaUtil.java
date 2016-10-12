package io.hops.kafkautil;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import io.hops.kafkautil.flink.FlinkConsumer;
import io.hops.kafkautil.flink.FlinkProducer;
import java.util.HashMap;
import java.util.Map;

import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import javax.ws.rs.core.Cookie;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
//import org.apache.flink.streaming.util.serialization.DeserializationSchema;
//import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
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

  private String jSessionId;
  private Integer projectId;
  private String brokerEndpoint;
  private String restEndpoint;
  private String keyStore;
  private String trustStore;

  private KafkaUtil() {

  }

  /**
   * Setup the Kafka instance.
   */
  public void setup() {
    Properties sysProps = System.getProperties();

    //validate arguments first
    this.jSessionId = sysProps.getProperty("kafka.sessionid");
    this.projectId = Integer.parseInt(sysProps.getProperty("kafka.projectid"));
    this.brokerEndpoint = sysProps.getProperty("kafka.brokeraddress");//"10.0.2.15:9091";
    this.restEndpoint = sysProps.getProperty("kafka.restendpoint");
    this.keyStore = "kafka_k_certificate";//sysProps.getProperty("kafka_k_certificate");
    this.trustStore = "kafka_t_certificate";//"sysProps.getProperty("kafka_t_certificate");
  }

  /**
   * Setup the Kafka instance.
   * Endpoint is where the REST API listens for requests. I.e.
   * http://localhost:8080/. Similarly set domain to "localhost"
   * <p>
   * @param endpoint
   * @param domain
   */
  public void setup(String endpoint, String domain) {
    Properties sysProps = System.getProperties();

    //validate arguments first
    this.jSessionId = sysProps.getProperty("kafka.sessionid");
    this.projectId = Integer.parseInt(sysProps.getProperty("kafka.projectid"));
    this.brokerEndpoint = sysProps.getProperty("kafka.brokeraddress");
    this.restEndpoint = endpoint + "/hopsworks/api/project";
    this.keyStore = "kafka_k_certificate";
    this.trustStore = "kafka_t_certificate";
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
   */
  public void setup(String topicName, String restEndpoint, String keyStore,
          String trustStore, String domain) {
    Properties sysProps = System.getProperties();

    //validate arguments first
    this.jSessionId = sysProps.getProperty("kafka.sessionid");
    this.projectId = Integer.parseInt(sysProps.getProperty("kafka.projectid"));
    this.brokerEndpoint = sysProps.getProperty("kafka.brokeraddress");
    this.restEndpoint = restEndpoint + "/hopsworks/api/project";
    this.keyStore = keyStore;
    this.trustStore = trustStore;
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
   */
  public void setup(String jSessionId, int projectId, String topicName,
          String domain, String brokerEndpoint, String restEndpoint,
          String keyStore, String trustStore) {
    this.jSessionId = jSessionId;
    this.projectId = projectId;
    this.brokerEndpoint = brokerEndpoint;
    this.restEndpoint = restEndpoint + "/hopsworks/api/project";
    this.keyStore = keyStore;
    this.trustStore = trustStore;
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
   */
  public void setup(String jSessionId, int projectId, String topicName,
          String brokerEndpoint, String restEndpoint,
          String keyStore, String trustStore) {
    this.jSessionId = jSessionId;
    this.projectId = projectId;
    this.brokerEndpoint = brokerEndpoint;
    this.restEndpoint = restEndpoint + "/hopsworks/api/project";
    this.keyStore = keyStore;
    this.trustStore = trustStore;
  }

  public static KafkaUtil getInstance() {
    if (instance == null) {
      instance = new KafkaUtil();
      instance.setup();
    }
    return instance;
  }

  public HopsConsumer getHopsConsumer(String topic) throws
          SchemaNotFoundException {
    return new HopsConsumer(topic);
  }

  public HopsProducer getHopsProducer(String topic) throws
          SchemaNotFoundException {
    return new HopsProducer(topic);
  }

  public FlinkConsumer getFlinkConsumer(String topic) {
    return getFlinkConsumer(topic, new AvroDeserializer(topic));
  }

  public FlinkConsumer getFlinkConsumer(String topic,
          DeserializationSchema deserializationSchema) {
    return new FlinkConsumer(topic, deserializationSchema,
            getConsumerConfig());
  }

  public FlinkProducer getFlinkProducer(String topic) {
    return getFlinkProducer(topic, new AvroDeserializer(topic));
  }

  public FlinkProducer getFlinkProducer(String topic,
          SerializationSchema serializationSchema) {
    return new FlinkProducer(topic, serializationSchema,
            KafkaProperties.defaultProps());
  }

  /**
   *
   * @param topic
   * @return
   */
  public AvroDeserializer getHopsAvroSchema(String topic) {
    return new AvroDeserializer(topic);
  }

  /**
   *
   * @param jsc
   * @param key
   * @param value
   * @param keyDecoder
   * @param valueDecoder
   * @param topics
   * @return
   */
  public JavaPairInputDStream<String, String> createDirectStream(
          JavaStreamingContext jsc, Class key, Class value, Class keyDecoder,
          Class valueDecoder, Set<String> topics) {
    Map<String, String> kafkaParams = getSparkConsumerConfigMap();
    JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.
            createDirectStream(jsc, key, value, keyDecoder, valueDecoder,
                    kafkaParams,
                    topics);
    return directKafkaStream;
  }

  /**
   * @Deprecated.
   * @return
   */
  protected Properties getProducerConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerEndpoint);
    props.put("metadata.broker.list", brokerEndpoint);
    props.put("client.id", "DemoProducer");
    props.put("key.serializer",
            "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");

    //configure the ssl parameters
    if (!(keyStore.isEmpty() && keyStore == null)) {
      props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStore);
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "adminpw");
      props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStore);
      props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "adminpw");
    }
    return props;
  }

  protected Map<String, String> getProducerConfigMap() {
    Properties props = getProducerConfig();
    Map<String, String> propsMap = new HashMap<>();
    for (final String name : props.stringPropertyNames()) {
      propsMap.put(name, props.getProperty(name));
    }
//    propsMap.put("zookeeper.connect", "10.0.2.15:2181");
//    propsMap.put("group.id", "1");
//    propsMap.put("zookeeper.connection.timeout.ms", "10000");
    return propsMap;
  }

  /**
   * @Deprecated.
   * @return
   */
  protected Properties getConsumerConfig() {
    Properties props = new Properties();
    props.put("metadata.broker.list", brokerEndpoint);
//    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerEndpoint);
//    props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
//    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
//    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.IntegerDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");

    //configure the ssl parameters
    if (trustStore != null && !trustStore.isEmpty()
            && keyStore != null && !keyStore.isEmpty()) {
      props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
              KafkaUtil.
              getInstance().getTrustStore());
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "adminpw");
      props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, KafkaUtil.
              getInstance().getKeyStore());
      props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "adminpw");
    }

    return props;
  }

  /**
   *
   * @return
   */
  protected Properties getSparkConsumerConfig() {
    Properties props = new Properties();
    props.put("metadata.broker.list", "10.0.2.15:9092");
//    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerEndpoint);
//    props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
//    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
//    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.IntegerDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");

//    //configure the ssl parameters
//    if (trustStore != null && !trustStore.isEmpty()
//            && keyStore != null && !keyStore.isEmpty()) {
//      props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
//      props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
//              KafkaUtil.
//              getInstance().getTrustStore());
//      props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "adminpw");
//      props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, KafkaUtil.
//              getInstance().getKeyStore());
//      props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "adminpw");
//    }
    return props;
  }

  protected Map<String, String> getSparkConsumerConfigMap() {
    Properties props = getSparkConsumerConfig();
    Map<String, String> propsMap = new HashMap<>();
    for (final String name : props.stringPropertyNames()) {
      propsMap.put(name, props.getProperty(name));
    }
    System.out.println("propsMap:" + propsMap);
    return propsMap;
  }

  public String getSchema(String topicName) throws SchemaNotFoundException {
    return getSchema(topicName, Integer.MIN_VALUE);
  }

  public String getSchema(String topicName, int versionId) throws
          SchemaNotFoundException {
    System.out.println("kafka.hopsutil.topicName:" + topicName);
    String uri = restEndpoint + "/" + projectId + "/kafka/schema/" + topicName;
    if (versionId > 0) {
      uri += "/" + versionId;
    }
    System.out.println("kafka.hopsutil.uri:" + uri);

    //Setup the REST client to retrieve the schema
//    BasicCookieStore cookieStore = new BasicCookieStore();
//    BasicClientCookie cookie = new BasicClientCookie("JSESSIONID", jSessionId);
//    cookie.setDomain(domain);
//    cookie.setPath("/");
//    cookieStore.addCookie(cookie);
//    HttpClient client = HttpClientBuilder.create().setDefaultCookieStore(
//            cookieStore).build();
//
//    final HttpGet request = new HttpGet(uri);
//
//    logger.log(Level.INFO, "brokerEndpoint:{0}:", brokerEndpoint);
//    logger.log(Level.INFO, "schema uri:{0}:", uri);
//    HttpResponse response = null;
//    try {
//      response = client.execute(request);
//    } catch (IOException ex) {
//      logger.log(Level.SEVERE, ex.getMessage());
//    }
//    logger.log(Level.INFO, "schema response:", response);
//    if (response == null) {
//      throw new SchemaNotFoundException("Could not reach schema endpoint");
//    } else if (response.getStatusLine().getStatusCode() != 200) {
//      throw new SchemaNotFoundException(response.getStatusLine().getStatusCode(),
//              "Schema is not found");
//    }
//    //logger.log(Level.INFO, "Response:{0}", response.toString());
//    StringBuilder result = new StringBuilder();
//    try {
//      BufferedReader rd = new BufferedReader(
//              new InputStreamReader(response.getEntity().getContent()));
//
//      String line;
//      while ((line = rd.readLine()) != null) {
//        result.append(line);
//      }
//    } catch (IOException ex) {
//      logger.log(Level.SEVERE, ex.getMessage());
//    }
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

  public Map<String, String> getKafkaProps(String propsStr) {
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
