package io.hops.util;

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
 * <p>
 */
public class KafkaProperties {

  public KafkaProperties() {
  }

  public Properties defaultProps() {

    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HopsUtil.getBrokerEndpoint());
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    //configure the ssl parameters
    props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, HopsUtil.getTrustStore());
    props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, HopsUtil.getTruststorePwd());
    props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, HopsUtil.getKeyStore());
    props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, HopsUtil.getKeystorePwd());
    props.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, HopsUtil.getKeystorePwd());

    return props;
  }

  /**
   * @Deprecated.
   * @return
   */
  public Properties getProducerConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HopsUtil.getBrokerEndpoint());
    props.put("client.id", "DemoProducer");
    props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    //configure the ssl parameters
    if (!Strings.isNullOrEmpty(HopsUtil.getTrustStore()) && !Strings.isNullOrEmpty(HopsUtil.getKeyStore())) {
      props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, HopsUtil.getTrustStore());
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, HopsUtil.getTruststorePwd());
      props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, HopsUtil.getKeyStore());
      props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, HopsUtil.getKeystorePwd());
    }
    return props;
  }

  /**
   *
   * @return
   */
  public Properties getConsumerConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HopsUtil.getBrokerEndpoint());
    if (HopsUtil.getConsumerGroups() != null && !HopsUtil.getConsumerGroups().isEmpty()) {
      props.put(ConsumerConfig.GROUP_ID_CONFIG, HopsUtil.getConsumerGroups().get(0));
    }
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
    props.
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    //configure the ssl parameters
    if (!Strings.isNullOrEmpty(HopsUtil.getTrustStore())
        && !Strings.isNullOrEmpty(HopsUtil.getKeyStore())) {
      props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, HopsUtil.getTrustStore());
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, HopsUtil.getTruststorePwd());
      props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, HopsUtil.getKeyStore());
      props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, HopsUtil.getKeystorePwd());
      props.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, HopsUtil.getKeystorePwd());
    }

    return props;
  }

  /**
   *
   * @param userProps
   * @return
   */
  public Properties getConsumerConfig(Properties userProps) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HopsUtil.getBrokerEndpoint());
    if (HopsUtil.getConsumerGroups() != null && !HopsUtil.getConsumerGroups().isEmpty()) {
      props.put(ConsumerConfig.GROUP_ID_CONFIG, HopsUtil.getConsumerGroups().get(0));
    }
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
    props.
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    //configure the ssl parameters
    if (!Strings.isNullOrEmpty(HopsUtil.getTrustStore())
        && !Strings.isNullOrEmpty(HopsUtil.getKeyStore())) {
      props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, HopsUtil.getTrustStore());
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, HopsUtil.getTruststorePwd());
      props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, HopsUtil.getKeyStore());
      props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, HopsUtil.getKeystorePwd());
      props.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, HopsUtil.getKeystorePwd());
    }

    //Overwrite default Kafka properties with the ones provided by the user from the Spark App
    if (userProps != null) {
      props.putAll(userProps);
    }
    return props;
  }

  /**
   *
   * @return
   */
  public Properties getSparkConsumerConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HopsUtil.getBrokerEndpoint());
    if (HopsUtil.getConsumerGroups() != null && !HopsUtil.getConsumerGroups().isEmpty()) {
      props.put(ConsumerConfig.GROUP_ID_CONFIG, HopsUtil.getConsumerGroups().get(0));
    }
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());

    //configure the ssl parameters
    if (!Strings.isNullOrEmpty(HopsUtil.getTrustStore())
        && !Strings.isNullOrEmpty(HopsUtil.getKeyStore())) {
      props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, HopsUtil.getTrustStore());
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, HopsUtil.getTruststorePwd());
      props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, HopsUtil.getKeyStore());
      props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, HopsUtil.getKeystorePwd());
      props.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, HopsUtil.getKeystorePwd());
    }
    return props;
  }

  /**
   *
   * @param userProps
   * @return
   */
  public Properties getSparkConsumerConfig(Properties userProps) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HopsUtil.getBrokerEndpoint());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, HopsUtil.getConsumerGroups().get(0));
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());

    //configure the ssl parameters
    if (!Strings.isNullOrEmpty(HopsUtil.getTrustStore())
        && !Strings.isNullOrEmpty(HopsUtil.getKeyStore())) {
      props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, HopsUtil.getTrustStore());
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, HopsUtil.getTruststorePwd());
      props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, HopsUtil.getKeyStore());
      props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, HopsUtil.getKeystorePwd());
      props.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, HopsUtil.getKeystorePwd());
    }

    //Overwrite default Kafka properties with the ones provided by the user from the Spark App
    if (userProps != null) {
      props.putAll(userProps);
    }

    return props;
  }

  /**
   *
   * @return
   */
  public Map<String, Object> getSparkConsumerConfigMap() {
    return getSparkConsumerConfigMap(null);
  }

  /**
   *
   * @return
   */
  public Map<String, String> getSparkStructuredStreamingKafkaProps() {
    //Create options map for kafka
    return getSparkStructuredStreamingKafkaProps(null);
  }

  /**
   *
   * @param userOptions
   * @return
   */
  public Map<String, String> getSparkStructuredStreamingKafkaProps(Map<String, String> userOptions) {
    //Create options map for kafka
    Map<String, String> options = new HashMap<>();
    options.put("kafka." + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HopsUtil.getBrokerEndpoint());
    options.put("subscribe", HopsUtil.getTopicsAsCSV());
    options.put("kafka." + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    options.put("kafka." + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, HopsUtil.getTrustStore());
    options.put("kafka." + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, HopsUtil.getTruststorePwd());
    options.put("kafka." + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, HopsUtil.getKeyStore());
    options.put("kafka." + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, HopsUtil.getKeystorePwd());
    options.put("kafka." + SslConfigs.SSL_KEY_PASSWORD_CONFIG, HopsUtil.getKeystorePwd());

    if (userOptions != null) {
      options.putAll(userOptions);
    }

    return options;
  }

  public Map<String, Object> getSparkConsumerConfigMap(Properties userProps) {
    Properties props = getSparkConsumerConfig(userProps);
    Map<String, Object> propsMap = new HashMap<>();
    for (final String name : props.stringPropertyNames()) {
      propsMap.put(name, props.getProperty(name));
    }
    return propsMap;
  }
}
