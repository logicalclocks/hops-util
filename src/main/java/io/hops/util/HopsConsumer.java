package io.hops.util;

import io.hops.util.exceptions.CredentialsNotFoundException;
import io.hops.util.exceptions.SchemaNotFoundException;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.generic.GenericRecord;

/**
 * Utility class to consume messages from the Kafka service.
 * To begin consuming messages, the user must call the consume method which
 * starts a new thread.
 */
public class HopsConsumer extends HopsProcess implements Runnable {

  private static final Logger LOG = Logger.getLogger(HopsConsumer.class.getName());

  private KafkaConsumer<Integer, String> consumer;
  private boolean consume;
  private BlockingQueue<String> messages;
  private final boolean callback = false;
  private final StringBuilder consumed = new StringBuilder();

  HopsConsumer(String topic) throws SchemaNotFoundException, CredentialsNotFoundException {
    super(HopsProcessType.CONSUMER, topic);
  }

  /**
   * Start thread for consuming Kafka messages.
   * 
   */
  public void consume() {
    this.consume = true;
    //new Thread(this).start();
    Properties props = Hops.getKafkaProperties().getConsumerConfig();
    consumer = new KafkaConsumer<>(props);
    //Subscribe to the Kafka topic
    consumer.subscribe(Collections.singletonList(topic));
    if (callback) {
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
          LOG.log(Level.FINE, "Consumer put into queue:{0}", record.value());
          try {
            messages.put(record.value());
          } catch (InterruptedException ex) {
            Logger.getLogger(HopsConsumer.class.getName()).
                log(Level.SEVERE, null, ex);
          }
          LOG.log(Level.FINE, "Consumer received message:{0}", genericRecord);
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
          LOG.log(Level.SEVERE, "Error while consuming records", ex);
        }
      }
    } else {
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
          consumed.append(record.value()).append("\n");
          LOG.log(Level.FINE, "Consumer received message:{0}", genericRecord);
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
          LOG.log(Level.SEVERE, "Error while consuming records", ex);
        }
      }
      consumer.close();
    }
  }

  /**
   * Stop the consuming thread
   */
  public void stopConsuming() {
    consume = false;
  }

  @Override
  public void run() {
    Properties props = Hops.getKafkaProperties().getConsumerConfig();
    consumer = new KafkaConsumer<>(props);
    //Subscribe to the Kafka topic
    consumer.subscribe(Collections.singletonList(topic));
    if (callback) {
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
          LOG.log(Level.FINE, "Consumer put into queue:{0}", record.value());
          try {
            messages.put(record.value());
          } catch (InterruptedException ex) {
            Logger.getLogger(HopsConsumer.class.getName()).
                log(Level.SEVERE, null, ex);
          }
          LOG.log(Level.FINE, "Consumer received message:{0}", genericRecord);
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
          LOG.log(Level.SEVERE, "Error while consuming records", ex);
        }
      }
    } else {
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
          consumed.append(record.value()).append("\n");
          LOG.log(Level.FINE, "Consumer received message:{0}", genericRecord);
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
          LOG.log(Level.SEVERE, "Error while consuming records", ex);
        }
      }
    }
  }

  @Override
  public void close() {
    consume = false;
  }

}
