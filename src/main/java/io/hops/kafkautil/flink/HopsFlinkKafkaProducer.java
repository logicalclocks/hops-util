package io.hops.kafkautil.flink;

import io.hops.kafkautil.HopsKafkaProperties;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 * Wrapper class for FlinkKafkaProducer.
 * <p>
 */
public class HopsFlinkKafkaProducer extends FlinkKafkaProducer09 {

  public HopsFlinkKafkaProducer(String topic,
          SerializationSchema serializationSchema) {
    super(topic, new KeyedSerializationSchemaWrapper<>(serializationSchema),
            HopsKafkaProperties.defaultProps());

  }

}
