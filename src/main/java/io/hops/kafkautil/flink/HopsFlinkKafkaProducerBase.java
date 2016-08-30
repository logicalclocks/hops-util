package io.hops.kafkautil.flink;

import io.hops.kafkautil.HopsKafkaProcess;
import io.hops.kafkautil.KafkaProcessType;
import io.hops.kafkautil.SchemaNotFoundException;

/**
 *
 * @author teo
 */
public class HopsFlinkKafkaProducerBase extends HopsKafkaProcess {

  public HopsFlinkKafkaProducerBase(KafkaProcessType type, String topic) throws
          SchemaNotFoundException {
    super(type, topic);
  }

}
