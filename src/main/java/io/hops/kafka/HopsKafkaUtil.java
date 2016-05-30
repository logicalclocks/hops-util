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

    static String jSessionId;
    static Integer projectId;
    static String topicName;
    static String brokerEndpoint;
    static String restEndpoint;// = "https://bbc1.sics.se:14004/hopsworks/api/project/";
    static String keyStore;
    static String trustStore;

    private HopsKafkaUtil(String jSessionId, Integer projectId, String topicName,
            String brokerEndpoint, String restEndpoint, String keyStore,
            String trustStore) {
        HopsKafkaUtil.jSessionId = jSessionId;
        HopsKafkaUtil.projectId = projectId;
        HopsKafkaUtil.topicName = topicName;
        HopsKafkaUtil.brokerEndpoint = brokerEndpoint;
        HopsKafkaUtil.restEndpoint = restEndpoint;
        HopsKafkaUtil.keyStore = keyStore;
        HopsKafkaUtil.trustStore = trustStore;
    }

    private HopsKafkaUtil(String jSessionId, Integer projectId, String topicName,
            String brokerEndpoint, String restEndpoint) {
        HopsKafkaUtil.jSessionId = jSessionId;
        HopsKafkaUtil.projectId = projectId;
        HopsKafkaUtil.topicName = topicName;
        HopsKafkaUtil.brokerEndpoint = brokerEndpoint;
        HopsKafkaUtil.restEndpoint = restEndpoint;
    }

    public static HopsKafkaUtil create(String jSessionId, Integer projectId, String topicName,
            String brokerEndpoint, String restEndpoint, String keyStore,
            String trustStore) {

      //validate arguments first
        return new HopsKafkaUtil(jSessionId, projectId, topicName, brokerEndpoint,
                restEndpoint, keyStore, trustStore);
    }

    public static HopsKafkaUtil create(String jSessionId, Integer projectId, String topicName,
            String brokerEndpoint, String restEndpoint) {

        return new HopsKafkaUtil(jSessionId, projectId, topicName, brokerEndpoint,
                restEndpoint);
    }

    public static Properties getProducerConfig() {

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

    public static Properties getConsumerConfig() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerEndpoint);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

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

    public static String getSchema() {
        return getSchema(-1);
    }

    public static String getSchema(int versionId) {
        
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
            ;
        }

        return response.getEntity(String.class);

    }
}
