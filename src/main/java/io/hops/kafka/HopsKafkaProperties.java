package io.hops.kafka;

import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;

/**
 *
 * @author misdess
 */
public class HopsKafkaProperties {

  public static final String TOPIC = "sics1";
  public static final String KAFKA_SERVER_URL = "localhost";
  public static final int KAFKA_SERVER_PORT = 9096;
  public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
  public static final int CONNECTION_TIMEOUT = 100000;
  public static final String TOPIC2 = "topic2";
  public static final String TOPIC3 = "topic3";
  public static final String CLIENT_ID = "SimpleConsumerDemoClient";
  public static String KAFKA_CONNECTSTR = "";
  public static String TRUSTSTORE_PWD = "adminpw";
  public static String KEYSTORE_PWD = "adminpw";
  public static String KAFKA_T_CERTIFICATE_LOCATION = "";
  public static String KAFKA_K_CERTIFICATE_LOCATION = "";

  private HopsKafkaProperties() {
  }

  public static Properties defaultProps() {

    Properties props = new Properties();
    props.setProperty("bootstrap.servers", HopsKafkaUtil.getInstance().
            getBrokerEndpoint());
    props.setProperty("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty("value.serializer",
            "org.apache.kafka.common.serialization.ByteArraySerializer");

    //configure the ssl parameters
    props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, HopsKafkaUtil.
            getInstance().getTrustStore());
    props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TRUSTSTORE_PWD);
    props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, HopsKafkaUtil.
            getInstance().getKeyStore());
    props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, KEYSTORE_PWD);

    return props;
  }
}
