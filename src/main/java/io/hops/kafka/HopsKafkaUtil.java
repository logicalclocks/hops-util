package io.hops.kafka;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;

/**
 *
 * @author misdess
 */
public class HopsKafkaUtil {

    private static final Logger logger = Logger.getLogger(HopsKafkaUtil.class.getName());

    private static HopsKafkaUtil instance = null;

    public String jSessionId;
    public Integer projectId;
    public String topicName;
    public String brokerEndpoint;
    public String restEndpoint;// = "https://bbc1.sics.se:14004/hopsworks/api/project/";
    public String keyStore;
    public String trustStore;

    private HopsKafkaUtil(){
        
    }
    
    /**
     * Setup the Kafka instance.
     * @param topicName
     */
    public void setup(String topicName) {
        Properties sysProps = System.getProperties();

        //validate arguments first
        this.jSessionId = sysProps.getProperty("kafka.sessionid");
        this.projectId = Integer.parseInt(sysProps.getProperty("kafka.projectid"));
        this.topicName = topicName;
        this.brokerEndpoint = "0.0.0.0:9092";
        this.restEndpoint = "https://bbc1.sics.se:14004/hopsworks/api/project/";
        this.keyStore = "kafka_k_certificate";//sysProps.getProperty("kafka_k_certificate");
        this.trustStore = "kafka_t_certificate";//"sysProps.getProperty("kafka_t_certificate");;
        
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
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

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
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
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

    public String getSchema() {
        String schema = "{\n" +
"    \"fields\": [\n" +
"        { \"name\": \"str1\", \"type\": \"string\" },\n" +
"        { \"name\": \"str2\", \"type\": \"string\" }\n" +
"    ],\n" +
"    \"name\": \"myrecord\",\n" +
"    \"type\": \"record\"\n" +
"}\n" +
"";
        return schema;
//return getSchema(Integer.MIN_VALUE);
    }

    public String getSchema(int versionId) {

        String uri = restEndpoint + projectId + "/kafka/schema/" + topicName;
        if (versionId > 0) {
            uri += "/" + versionId;
        }
        System.out.println("Kafka.uri - "+uri);
        Client client = Client.create();
        WebResource resource = client.resource(uri);
        WebResource.Builder builder = resource.getRequestBuilder();
        builder.header("Authorization", jSessionId);
        System.out.println("Kafka.SessionId - "+jSessionId);
        ClientResponse response = builder.accept("application/json").get(ClientResponse.class);

        if (response.getStatus() != 200) {
            //TODO: Implement and throw SchemaNotFound exception
            ;
        }

        return response.getEntity(String.class);

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
