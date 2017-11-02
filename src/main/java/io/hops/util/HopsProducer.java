package io.hops.util;

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
   * @param topic
   * @param userProps
   * @throws SchemaNotFoundException
   * @throws io.hops.util.CredentialsNotFoundException
   */
  public HopsProducer(String topic, Properties userProps) throws SchemaNotFoundException, CredentialsNotFoundException {
    super(HopsProcessType.PRODUCER, topic);
    Properties props = HopsUtil.getKafkaProperties().defaultProps();
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "HopsProducer");
     if(userProps != null){
      props.putAll(userProps);
    }
    producer = new KafkaProducer<>(props);
    recordInjection = GenericAvroCodecs.toBinary(schema);
  }

  /**
   * Send the given record to Kafka.
   * <p>
   * @param messageFields
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
