package io.hops.kafkautil;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import io.hops.kafkautil.flink.FlinkConsumer;
import io.hops.kafkautil.flink.FlinkProducer;
import io.hops.kafkautil.spark.SparkConsumer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import java.util.Properties;
import java.util.logging.Logger;
import javax.ws.rs.core.Cookie;
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
//  private final Map<String, Schema> schemas = new HashMap<>();

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
    this.restEndpoint = sysProps.getProperty("kafka.restendpoint")
            + "/hopsworks/api/project";
    this.keyStore = "kafka_k_certificate";//sysProps.getProperty("kafka_k_certificate");
    this.trustStore = "kafka_t_certificate";//"sysProps.getProperty("kafka_t_certificate");
    isSetup = true;
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
    isSetup = true;
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
    isSetup = true;
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
    isSetup = true;
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
    isSetup = true;
  }

  public static KafkaUtil getInstance() {
    if (instance == null) {
      instance = new KafkaUtil();
      if (!isSetup) {
        instance.setup();
      }
    }
    return instance;
  }

  public KafkaProperties getKafkaProperties() {
    return new KafkaProperties();
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
            getKafkaProperties().getConsumerConfig());
  }

  public FlinkProducer getFlinkProducer(String topic) {
    return getFlinkProducer(topic, new AvroDeserializer(topic));
  }

  public FlinkProducer getFlinkProducer(String topic,
          SerializationSchema serializationSchema) {
    return new FlinkProducer(topic, serializationSchema,
            getKafkaProperties().defaultProps());
  }

  public SparkConsumer getSparkConsumer(JavaStreamingContext jsc,
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
