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
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 *
 * Utility class that sends messages to the Kafka service.
 */
public class HopsProducer extends HopsProcess {

  private static final Logger LOGGER = Logger.getLogger(HopsProducer.class.getName());

  private final KafkaProducer<String, byte[]> producer;
  private final Injection<GenericRecord, byte[]> recordInjection;
  private GenericData.Record avroRecord;
  private ProducerRecord<String, byte[]> record;

  /**
   * Create a Producer to stream messages to Kafka.
   *
   * @param topic Kafka topic.
   * @param userProps User-provided properties.
   * @throws SchemaNotFoundException When Avro schema for topic could not be found in HopsWorks.
   * @throws io.hops.util.exceptions.CredentialsNotFoundException CredentialsNotFoundException
   */
  public HopsProducer(String topic, Properties userProps) throws SchemaNotFoundException, CredentialsNotFoundException {
    super(HopsProcessType.PRODUCER, topic);
    Properties props = Hops.getKafkaProperties().defaultProps();
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "HopsProducer");
    if (userProps != null) {
      props.putAll(userProps);
    }
    producer = new KafkaProducer<>(props);
    recordInjection = GenericAvroCodecs.toBinary(schema);
  }

  /**
   * Send the given record to Kafka.
   * 
   * @param messageFields message to produce to Kafka
   */
  public void produce(Map<String, String> messageFields) {
    //create the avro message
    avroRecord = new GenericData.Record(schema);
    for (Map.Entry<String, String> message : messageFields.entrySet()) {
      //TODO: Check that messageFields are in avro record
      avroRecord.put(message.getKey(), message.getValue());
    }

    byte[] bytes = recordInjection.apply(avroRecord);
    record = new ProducerRecord<>(topic, bytes);
    producer.send(record);

    LOGGER.log(Level.INFO, "Producer sent message: {0}", messageFields);
  }

  @Override
  public void close() {
    producer.close();
  }

}
