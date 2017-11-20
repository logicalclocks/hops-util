package io.hops.util.dela;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import io.hops.util.HopsProcess;
import io.hops.util.HopsProcessType;
import io.hops.util.HopsUtil;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Hops Dela wrapper for a Kafka consumer.
 */
public class DelaConsumer extends HopsProcess implements Runnable {

  private static final Logger LOGGER = Logger.getLogger(DelaConsumer.class.getName());

  private KafkaConsumer<Integer, String> consumer;
  private boolean consume;

  /**
   *
   * @param topic
   * @param schema
   */
  public DelaConsumer(String topic, Schema schema) {
    super(HopsProcessType.CONSUMER, topic, schema);
  }

  /**
   *
   */
  public void consume() {
    consume = true;
    new Thread(this).start();
  }

  /**
   *
   */
  public void stopConsuming() {
    consume = false;
  }

  /**
   *
   */
  @Override
  public void run() {
    Properties props = HopsUtil.getKafkaProperties().getConsumerConfig();
    consumer = new KafkaConsumer<>(props);
    //Subscribe to the Kafka topic
    consumer.subscribe(Collections.singletonList(topic));
    while (consume) {
      ConsumerRecords<Integer, String> records = consumer.poll(1000);
      //Get the records
      for (ConsumerRecord<Integer, String> record : records) {
        //Convert the record using the schema
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        GenericRecord genericRecord = recordInjection.invert(record.value().getBytes()).get();
        LOGGER.log(Level.INFO, "Consumer received message:{0}", genericRecord);
      }
    }
  }

  /**
   *
   */
  @Override
  public void close() {
    consume = false;
  }
}
