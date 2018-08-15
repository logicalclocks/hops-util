/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * Default Kafka properties class.
 *
 */
public class KafkaProperties {

  public KafkaProperties() {
  }

  public Properties defaultProps() {

    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Hops.getBrokerEndpoints());
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    //configure the ssl parameters
    props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, Hops.getTrustStore());
    props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, Hops.getTruststorePwd());
    props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, Hops.getKeyStore());
    props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, Hops.getKeystorePwd());
    props.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, Hops.getKeystorePwd());

    return props;
  }

  /**
   * Get Consumer properties.
   *
   * @return Properties
   */
  public Properties getConsumerConfig() {
    return getConsumerConfig(null);
  }

  /**
   * Get Consumer properties.
   *
   * @param userProps User-provided properties.
   * @return Properties.
   */
  public Properties getConsumerConfig(Properties userProps) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Hops.getBrokerEndpointsList());
    if (Hops.getConsumerGroups() != null && !Hops.getConsumerGroups().isEmpty()) {
      props.put(ConsumerConfig.GROUP_ID_CONFIG, Hops.getConsumerGroups().get(0));
    }
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.IntegerDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");

    //configure the ssl parameters
    if (!Strings.isNullOrEmpty(Hops.getTrustStore())
        && !Strings.isNullOrEmpty(Hops.getKeyStore())) {
      props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, Hops.getTrustStore());
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, Hops.getTruststorePwd());
      props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, Hops.getKeyStore());
      props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, Hops.getKeystorePwd());
      props.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, Hops.getKeystorePwd());
    }

    //Overwrite default Kafka properties with the ones provided by the user from the Spark App
    if (userProps != null) {
      props.putAll(userProps);
    }
    return props;
  }

  public Properties getSparkConsumerConfig() {
    return getSparkConsumerConfig(null);
  }

  public Properties getSparkConsumerConfig(Properties userProps) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Hops.getBrokerEndpoints());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, Hops.getConsumerGroups().get(0));
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());

    //configure the ssl parameters
    if (!Strings.isNullOrEmpty(Hops.getTrustStore())
        && !Strings.isNullOrEmpty(Hops.getKeyStore())) {
      props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, Hops.getTrustStore());
      props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, Hops.getTruststorePwd());
      props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, Hops.getKeyStore());
      props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, Hops.getKeystorePwd());
      props.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, Hops.getKeystorePwd());
    }

    //Overwrite default Kafka properties with the ones provided by the user from the Spark App
    if (userProps != null) {
      props.putAll(userProps);
    }

    return props;
  }

  public Map<String, Object> getSparkConsumerConfigMap() {
    return getSparkConsumerConfigMap(null);
  }

  public Map<String, String> getSparkStructuredStreamingKafkaProps() {
    //Create options map for kafka
    return getSparkStructuredStreamingKafkaProps(null);
  }

  public Map<String, String> getSparkStructuredStreamingKafkaProps(Map<String, String> userOptions) {
    //Create options map for kafka
    Map<String, String> options = new HashMap<>();
    options.put("kafka." + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Hops.getBrokerEndpoints());
    options.put("subscribe", Hops.getTopicsAsCSV());
    options.put("kafka." + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    options.put("kafka." + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, Hops.getTrustStore());
    options.put("kafka." + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, Hops.getTruststorePwd());
    options.put("kafka." + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, Hops.getKeyStore());
    options.put("kafka." + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, Hops.getKeystorePwd());
    options.put("kafka." + SslConfigs.SSL_KEY_PASSWORD_CONFIG, Hops.getKeystorePwd());

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
