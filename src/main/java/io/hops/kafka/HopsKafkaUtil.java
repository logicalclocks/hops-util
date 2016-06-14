package io.hops.kafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.json.JSONObject;

/**
 * Hops utility class to be used by applications that want to communicate
 * with Kafka.
 *  
 */
public class HopsKafkaUtil {

  private static final Logger logger = Logger.getLogger(HopsKafkaUtil.class.
          getName());

  private static HopsKafkaUtil instance = null;

  public String jSessionId;
  public Integer projectId;
  public String topicName;
  public String brokerEndpoint;
  public String restEndpoint;
  public String keyStore;
  public String trustStore;

  private HopsKafkaUtil() {

  }

  /**
   * Setup the Kafka instance.
   * 
   * @param topicName
   */
  public void setup(String topicName) {
    Properties sysProps = System.getProperties();

    //validate arguments first
    this.jSessionId = sysProps.getProperty("kafka.sessionid");
    this.projectId = Integer.parseInt(sysProps.getProperty("kafka.projectid"));
    this.topicName = topicName;
    this.brokerEndpoint = sysProps.getProperty("kafka.brokeraddress");//"10.0.2.15:9091";
    this.restEndpoint = "http://tkak2.sics.se:8080/hopsworks/api/project";
    this.keyStore = "kafka_k_certificate";//sysProps.getProperty("kafka_k_certificate");
    this.trustStore = "kafka_t_certificate";//"sysProps.getProperty("kafka_t_certificate");;
    logger.log(Level.INFO, "brokerEndpoint:{0}", brokerEndpoint);
    logger.log(Level.INFO, "trustStore:{0}", trustStore);
    logger.log(Level.INFO, "keyStore:{0}", keyStore);
  }

  public static HopsKafkaUtil getInstance() {
    if (instance == null) {
      instance = new HopsKafkaUtil();
    }
    return instance;
  }

  public Properties getProducerConfig() {

    Properties props = new Properties();
    props.put("bootstrap.servers", brokerEndpoint);
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

  public Properties getConsumerConfig() {

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerEndpoint);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.IntegerDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
    
    //configure the ssl parameters
    if (trustStore != null && !trustStore.isEmpty()
            && keyStore != null && !keyStore.isEmpty()) {
      props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStore);
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "adminpw");
      props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStore);
      props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "adminpw");
    }

    return props;
  }

  public String getSchema() throws SchemaNotFoundException {
    return getSchema(Integer.MIN_VALUE);
  }

  public String getSchema(int versionId) throws SchemaNotFoundException {

    String uri = restEndpoint + "/" + projectId + "/kafka/schema/" + topicName;
    if (versionId > 0) {
      uri += "/" + versionId;
    }
    //Client client = Client.create();
    BasicCookieStore cookieStore = new BasicCookieStore();
    BasicClientCookie cookie = new BasicClientCookie("SESSIONID", jSessionId);
    cookie.setDomain("tkak2.sics.se");
    cookie.setPath("/");
    cookieStore.addCookie(cookie);
    HttpClient client = HttpClientBuilder.create().setDefaultCookieStore(
            cookieStore).build();

    final HttpGet request = new HttpGet(uri);

    HttpResponse response = null;
    try {
      response = client.execute(request);
    } catch (IOException ex) {
      logger.log(Level.SEVERE, ex.getMessage());
    }
    logger.log(Level.INFO, "Response:{0}", response.toString());
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new SchemaNotFoundException(response.getStatusLine().getStatusCode(),
              "Schema is not found");
    }
    StringBuilder result = new StringBuilder();
    try {
      BufferedReader rd = new BufferedReader(
              new InputStreamReader(response.getEntity().getContent()));

      String line;
      while ((line = rd.readLine()) != null) {
        result.append(line);
      }
    } catch (IOException ex) {
      logger.log(Level.SEVERE, ex.getMessage());
    }

    //Result is a json object with information about the schema. Extract the 
    //schema and return it.
    //Remove "[" and "]" characters
    String malformedJSON = result.toString();
    if(malformedJSON != null){
      if(malformedJSON.startsWith("[")){
        malformedJSON = malformedJSON.substring(1);
      }
      if(malformedJSON.endsWith("]")){
        malformedJSON = malformedJSON.substring(0, malformedJSON.length()-1);
      }
    }
    
    JSONObject json = new JSONObject(malformedJSON);
    String schema = json.getString("contents");

    logger.log(Level.INFO, "Result:{0}", result.toString());
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

  public String getTopicName() {
    return topicName;
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

}
