package io.hops.kafkautil;

import org.apache.avro.Schema;

/**
 * Defines the common characteristics of Kafka processes.
 * <p>
 */
public abstract class HopsKafkaProcess {

  public KafkaProcessType type;
  final String topic;
  final Schema schema;
  private final HopsKafkaUtil hopsKafkaUtil = HopsKafkaUtil.getInstance();
  
  /**
   * 
   * @param type
   * @param topic
   * @throws SchemaNotFoundException 
   */
  public HopsKafkaProcess(KafkaProcessType type, String topic) throws SchemaNotFoundException{
    this.topic = topic;
    Schema.Parser parser = new Schema.Parser();
    schema = parser.parse(hopsKafkaUtil.getSchema());
    
  }

}
