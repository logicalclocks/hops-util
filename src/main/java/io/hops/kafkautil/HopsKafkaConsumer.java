package io.hops.kafkautil;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.generic.GenericRecord;

/**
 * Utility class to consume messages from the Kafka service.
 * To begin consuming messages, the user must call the consume method which
 * starts a new thread.
 */
public class HopsKafkaConsumer extends HopsKafkaProcess implements Runnable {

  private static final Logger logger = Logger.getLogger(HopsKafkaConsumer.class.
          getName());

  private final KafkaConsumer<Integer, String> consumer;
  HopsKafkaUtil hopsKafkaUtil = HopsKafkaUtil.getInstance();
  private boolean consume;

  HopsKafkaConsumer(String topic) throws SchemaNotFoundException {
    super(KafkaProcessType.CONSUMER, topic);
    //Get Consumer properties
    Properties props = HopsKafkaUtil.getInstance().getConsumerConfig();
    consumer = new KafkaConsumer<>(props);
  }

  /**
   * Start thread for consuming Kafka messages.
   */
  public void consume() {
    consume = true;
    new Thread(this).start();
  }

  /**
   * Stop the consuming thread
   */
  public void stopConsuming() {
    consume = false;
  }

  @Override
  public void run() {
    //Subscribe to the Kafka topic
    consumer.subscribe(Collections.singletonList(topic));
    while (consume) {
      ConsumerRecords<Integer, String> records = consumer.poll(1000);
      //synchronized(kafkaRecords){
      //Get the records
      for (ConsumerRecord<Integer, String> record : records) {
        //Convert the record using the schema
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.
                toBinary(schema);
        GenericRecord genericRecord = recordInjection.invert(record.value().
                getBytes()).get();
        System.out.println("Consumer received message:" + genericRecord);
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        logger.log(Level.SEVERE, "Error while consuming records", ex);
      }
    }
  }

}
