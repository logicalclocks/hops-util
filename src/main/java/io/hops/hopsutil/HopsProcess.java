package io.hops.hopsutil;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;

/**
 * Defines the common characteristics of Kafka processes.
 * <p>
 */
public abstract class HopsProcess implements Serializable{

  private static final Logger logger = Logger.
          getLogger(HopsProcess.class.getName());
  public HopsProcessType type;
  final String topic;
  final Schema schema;
  private final HopsUtil hopsKafkaUtil = HopsUtil.getInstance();

  /**
   *
   * @param type
   * @param topic
   * @throws SchemaNotFoundException
   */
  public HopsProcess(HopsProcessType type, String topic) throws
          SchemaNotFoundException {
    this.topic = topic;
    Schema.Parser parser = new Schema.Parser();
    logger.log(Level.INFO, "Trying to get schema for topic:{0}", topic);

    schema = parser.parse(hopsKafkaUtil.getSchema(topic));
    logger.log(Level.INFO, "Got schema:{0}", schema);

  }

  /**
   * Closes the Kafka process.
   */
  public abstract void close();
}
