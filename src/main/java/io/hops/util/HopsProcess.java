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

    schema = parser.parse(Hops.getSchema(topic));
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
