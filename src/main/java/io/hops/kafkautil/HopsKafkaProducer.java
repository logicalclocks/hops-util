package io.hops.kafkautil;

import org.apache.avro.Schema;
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

/**
 *
 * Utility class that sends messages to the Kafka service.
 */
public class HopsKafkaProducer extends HopsKafkaProcess {

  private static final Logger logger = Logger.
          getLogger(HopsKafkaProducer.class.getName());

  private final KafkaProducer<String, byte[]> producer;
  private final Injection<GenericRecord, byte[]> recordInjection;

  /**
   * Create a Producer to stream messages to Kafka.
   *
   * @throws SchemaNotFoundException
   */
  HopsKafkaProducer(String topic) throws SchemaNotFoundException {
    super(KafkaProcessType.PRODUCER, topic);
    Properties props = HopsKafkaProperties.defaultProps();
    props.put("client.id", "HopsProducer");
    producer = new KafkaProducer<>(props);
    logger.log(Level.INFO, "Trying to get schema for topic:{0}", topic);
    logger.log(Level.INFO, "Got schema:{0}", schema);
    recordInjection = GenericAvroCodecs.toBinary(schema);
  }

  /**
   * Send the given record to Kafka.
   * <p>
   * @param messageFields
   */
  public void produce(Map<String, Object> messageFields) {
    //create the avro message
    GenericData.Record avroRecord = new GenericData.Record(schema);
    for (Map.Entry<String, Object> message : messageFields.entrySet()) {
      //TODO: Check that messageFields are in avro record
      avroRecord.put(message.getKey(), message.getValue());
    }

    byte[] bytes = recordInjection.apply(avroRecord);
    ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, bytes);
    producer.send(record);

    logger.log(Level.INFO, "Producer sent message: {0}", messageFields);
  }
}

/*
 * class DemoCallBack implements Callback {
 *
 * private static final Logger logger = Logger.
 * getLogger(DemoCallBack.class.getName());
 *
 * private final long startTime;
 * private final int key;
 * private final String message;
 *
 * public DemoCallBack(long startTime, int key, String message) {
 * this.startTime = startTime;
 * this.key = key;
 * this.message = message;
 * }
 */
/**
 * A callback method the user can implement to provide asynchronous handling
 * of request completion. This method will be called when the record sent to
 * the server has been acknowledged. Exactly one of the arguments will be
 * non-null.
 *
 * @param metadata The metadata for the record that was sent (i.e. the
 * partition and offset). Null if an error occurred.
 * @param exception The exception thrown during processing of this record.
 * Null if no error occurred.
 */
/*
 * public void onCompletion(RecordMetadata metadata, Exception exception) {
 *
 * if (metadata != null) {
 * logger.log(Level.SEVERE, "Message {0} is sent",
 * new Object[]{message});
 * } else {
 * exception.printStackTrace();
 * }
 * }
 * }
 */
