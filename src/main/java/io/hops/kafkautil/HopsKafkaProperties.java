package io.hops.kafkautil;

import java.io.Serializable;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;

/**
 *
 * @author misdess
 */
public class HopsKafkaProperties implements Serializable {

  public static String TRUSTSTORE_PWD = "adminpw";
  public static String KEYSTORE_PWD = "adminpw";

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
