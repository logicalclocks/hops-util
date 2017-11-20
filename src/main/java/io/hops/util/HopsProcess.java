package io.hops.util;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;

/**
 * Defines the common characteristics of Kafka processes.
 * <p>
 */
public abstract class HopsProcess implements Serializable {

  private static final Logger LOGGER = Logger.getLogger(HopsProcess.class.getName());
  public HopsProcessType type;
  protected final String topic;
  protected final Schema schema;

  /**
   *
   * @param type
   * @param topic
   * @throws SchemaNotFoundException
   * @throws io.hops.util.CredentialsNotFoundException
   */
  public HopsProcess(HopsProcessType type, String topic) throws
      SchemaNotFoundException, CredentialsNotFoundException {
    this.topic = topic;
    Schema.Parser parser = new Schema.Parser();
    LOGGER.log(Level.INFO, "Trying to get schema for topic:{0}", topic);

    schema = parser.parse(HopsUtil.getSchema(topic));
    LOGGER.log(Level.INFO, "Got schema:{0}", schema);

  }
  
  public HopsProcess(HopsProcessType type, String topic, Schema schema) {
    this.type = type;
    this.topic = topic;
    this.schema = schema;
  }

  /**
   * Closes the Kafka process.
   */
  public abstract void close();
  
  public Schema getSchema() {
    return schema;
  }
}
