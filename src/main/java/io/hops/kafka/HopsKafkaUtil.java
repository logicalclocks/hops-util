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
     * @param jSessionId
     * @param projectId
     * @param topicName
     * @param brokerEndpoint
     * @param restEndpoint
     * @param keyStore
     * @param trustStore 
     */
    public void setup(String jSessionId, Integer projectId, String topicName,
            String brokerEndpoint, String restEndpoint, String keyStore,
            String trustStore) {
        //validate arguments first
        this.jSessionId = jSessionId;
        this.projectId = projectId;
        this.topicName = topicName;
        this.brokerEndpoint = brokerEndpoint;
        this.restEndpoint = restEndpoint;
        this.keyStore = keyStore;
        this.trustStore = trustStore;
    }

    /**
     * Setup the Kafka instance.
     * @param jSessionId
     * @param projectId
     * @param topicName
     * @param brokerEndpoint
     * @param restEndpoint 
     */
    public void setup(String jSessionId, Integer projectId, String topicName,
            String brokerEndpoint, String restEndpoint) {
       setup(jSessionId, projectId, topicName, brokerEndpoint, restEndpoint, 
               null, null);
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
            props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "pass:adminpw");
            props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStore);
            props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "pass:adminpw");
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
            props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "pass:adminpw");
            props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStore);
            props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "pass:adminpw");
        }

        return props;
    }

    public String getSchema() {
        return getSchema(Integer.MIN_VALUE);
    }

    public String getSchema(int versionId) {

        String uri = restEndpoint + projectId + "/kafka/schema/" + topicName;
        if (versionId > 0) {
            uri += "/" + versionId;
        }
        Client client = Client.create();
        WebResource resource = client.resource(uri);
        WebResource.Builder builder = resource.getRequestBuilder();
        builder.header("Authorization", jSessionId);

        ClientResponse response = builder.accept("application/json").get(ClientResponse.class);

        if (response.getStatus() != 200) {
            //TODO: Implement and throw SchemaNotFound exception
            ;
        }

        return response.getEntity(String.class);

    }
}
