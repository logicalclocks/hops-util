package io.hops.kafkautil.flink;

import java.util.Properties;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

/**
 *
 * Hops Wrapper for FlinkKafkaConsumer.
 */
public class FlinkConsumer extends FlinkKafkaConsumer09 {

  public FlinkConsumer(String topic, DeserializationSchema schema,
          Properties props) {
    super(topic, schema, props);
  }

}
