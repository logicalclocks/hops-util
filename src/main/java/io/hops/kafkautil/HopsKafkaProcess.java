package io.hops.kafkautil;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;

/**
 * Defines the common characteristics of Kafka processes.
 * <p>
 */
public abstract class HopsKafkaProcess {

  private static final Logger logger = Logger.
          getLogger(HopsKafkaProcess.class.getName());
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
  public HopsKafkaProcess(KafkaProcessType type, String topic) throws
          SchemaNotFoundException {
    this.topic = topic;
    Schema.Parser parser = new Schema.Parser();
    logger.log(Level.INFO, "Trying to get schema for topic:{0}", topic);

    schema = parser.parse(hopsKafkaUtil.getSchema(topic));
    logger.log(Level.INFO, "Got schema:{0}", schema);

  }

}
