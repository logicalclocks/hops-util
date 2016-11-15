package io.hops.kafkautil.flink;

import java.util.Properties;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 * Wrapper class for FlinkKafkaProducer.
 * <p>
 */
public class FlinkProducer extends FlinkKafkaProducer09 {

  public FlinkProducer(String topic,
          SerializationSchema serializationSchema,
          Properties props) {
    super(topic, new KeyedSerializationSchemaWrapper<>(serializationSchema),
            props);
  
  }

}
