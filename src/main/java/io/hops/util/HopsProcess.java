package io.hops.util;

import io.hops.util.exceptions.CredentialsNotFoundException;
import io.hops.util.exceptions.SchemaNotFoundException;
import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;

/**
 * Defines the common characteristics of Kafka processes.
 * 
 */
public abstract class HopsProcess implements Serializable {

  private static final Logger LOGGER = Logger.getLogger(HopsProcess.class.getName());
  public HopsProcessType type;
  protected final String topic;
  protected final Schema schema;

  /**
   *
   * @param type HopsProcessType
   * @param topic Kafka topic
   * @throws SchemaNotFoundException SchemaNotFoundException
   * @throws io.hops.util.exceptions.CredentialsNotFoundException CredentialsNotFoundException
   */
  public HopsProcess(HopsProcessType type, String topic) throws
      SchemaNotFoundException, CredentialsNotFoundException {
    this.topic = topic;
    Schema.Parser parser = new Schema.Parser();
    LOGGER.log(Level.INFO, "Trying to get schema for topic:{0}", topic);

    schema = parser.parse(HopsUtil.getSchema(topic));
    LOGGER.log(Level.INFO, "Got schema:{0}", schema);

  }
  
  /**
   * 
   * @param type HopsProcessType
   * @param topic Kafka topic
   * @param schema Avro schema
   */
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
