package io.hops.hopsutil;

import com.google.common.base.Strings;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 *
 * @author misdess
 */
public class KafkaProperties {

  public static String TRUSTSTORE_PWD = "adminpw";
  public static String KEYSTORE_PWD = "adminpw";

  public KafkaProperties() {
  }

  public Properties defaultProps() {

    Properties props = new Properties();
    props.setProperty("bootstrap.servers", HopsUtil.getInstance().
            getBrokerEndpoint());
    props.setProperty("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty("value.serializer",
            "org.apache.kafka.common.serialization.ByteArraySerializer");

    //configure the ssl parameters
    props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, HopsUtil.
            getInstance().getTrustStore());
    props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, TRUSTSTORE_PWD);
    props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, HopsUtil.
            getInstance().getKeyStore());
    props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, KEYSTORE_PWD);

    return props;
  }

  /**
   * @Deprecated.
   * @return
   */
  public Properties getProducerConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HopsUtil.getInstance().
            getBrokerEndpoint());
    props.put("client.id", "DemoProducer");
    props.put("key.serializer",
            "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");

    //configure the ssl parameters
    if (!Strings.isNullOrEmpty(HopsUtil.getInstance().getTrustStore())
            && !Strings.isNullOrEmpty(HopsUtil.getInstance().getKeyStore())) {
      props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, HopsUtil.
              getInstance().getTrustStore());
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "adminpw");
      props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, HopsUtil.
              getInstance().getKeyStore());
      props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "adminpw");
    }
    return props;
  }

  /**
   * @Deprecated.
   * @return
   */
  public Properties getConsumerConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HopsUtil.getInstance().
            getBrokerEndpoint());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.IntegerDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");

    //configure the ssl parameters
    if (!Strings.isNullOrEmpty(HopsUtil.getInstance().getTrustStore())
            && !Strings.isNullOrEmpty(HopsUtil.getInstance().getKeyStore())) {
      props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, HopsUtil.
              getInstance().getTrustStore());
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "adminpw");
      props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, HopsUtil.
              getInstance().getKeyStore());
      props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "adminpw");
    }

    return props;
  }

  /**
   *
   * @return
   */
  public Properties getSparkConsumerConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HopsUtil.getInstance().
            getBrokerEndpoint());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getCanonicalName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getCanonicalName());

    //configure the ssl parameters
    if (!Strings.isNullOrEmpty(HopsUtil.getInstance().getTrustStore())
            && !Strings.isNullOrEmpty(HopsUtil.getInstance().getKeyStore())) {
      props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, HopsUtil.
              getInstance().getTrustStore());
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "adminpw");
      props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, HopsUtil.
              getInstance().getKeyStore());
      props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "adminpw");
    }
    return props;
  }

  public Map<String, Object> getSparkConsumerConfigMap() {
    Properties props = getSparkConsumerConfig();
    Map<String, Object> propsMap = new HashMap<>();
    for (final String name : props.stringPropertyNames()) {
      propsMap.put(name, props.getProperty(name));
    }
    return propsMap;
  }
}
