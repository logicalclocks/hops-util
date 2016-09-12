package io.hops.kafkautil;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.hadoop.fs.FSDataOutputStream;
import java.util.Collections; 
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Utility class to consume messages from the Kafka service.
 * To begin consuming messages, the user must call the consume method which
 * starts a new thread.
 */
public class HopsConsumer extends HopsProcess implements Runnable {

  private static final Logger logger = Logger.getLogger(HopsConsumer.class.
          getName());

  private KafkaConsumer<Integer, String> consumer;
  KafkaUtil hopsKafkaUtil = KafkaUtil.getInstance();
  private boolean consume;
  private BlockingQueue<String> messages;
  private boolean callback = false;
  private StringBuilder consumed = new StringBuilder();

  HopsConsumer(String topic) throws SchemaNotFoundException {
    super(HopsProcessType.CONSUMER, topic);
    //Get Consumer properties
    //Properties props = KafkaUtil.getInstance().getConsumerConfig();
    //consumer = new KafkaConsumer<>(props);
  }

  /**
   * Start thread for consuming Kafka messages.
   *
   * @param path
   */
  public void consume(String path) {
    this.consume = true;
    //new Thread(this).start();
    Properties props = KafkaUtil.getInstance().getConsumerConfig();
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
          System.out.println("Consumer put into queue:" + record.value());
          try {
            messages.put(record.value());
          } catch (InterruptedException ex) {
            Logger.getLogger(HopsConsumer.class.getName()).
                    log(Level.SEVERE, null, ex);
          }
          System.out.println("Consumer received message:" + genericRecord);
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
          logger.log(Level.SEVERE, "Error while consuming records", ex);
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
          System.out.println("Consumer received message:" + genericRecord);
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
          logger.log(Level.SEVERE, "Error while consuming records", ex);
        }
      }
      consumer.close();
      if (path != null && consumed.length() > 0) {
        try {
          Configuration hdConf = new Configuration();
          Path hdPath = new org.apache.hadoop.fs.Path(path);
          FileSystem hdfs = hdPath.getFileSystem(hdConf);
          FSDataOutputStream stream = hdfs.create(hdPath);
          stream.write(consumed.toString().getBytes());
          stream.flush();
          stream.close();

        } catch (IOException ex) {
          Logger.getLogger(HopsConsumer.class.getName()).
                  log(Level.SEVERE, null, ex);
        }
      }
    }
  }

  public void consume() {
    consume(null);
  }

//  public void consume(BlockingQueue messages) {
//    this.messages = messages;
//    callback = true;
//    consume = true;
//    new Thread(this).start();
//  }

//  public void consume(FSDataOutputStream stream) {
//    this.stream = stream;
//    callback = true;
//    consume = true;
//    new Thread(this).start();
//  }
  /**
   * Stop the consuming thread
   */
  public void stopConsuming() {
    consume = false;
  }

  @Override
  public void run() {
    Properties props = KafkaUtil.getInstance().getConsumerConfig();
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
          System.out.println("Consumer put into queue:" + record.value());
          try {
            messages.put(record.value());
          } catch (InterruptedException ex) {
            Logger.getLogger(HopsConsumer.class.getName()).
                    log(Level.SEVERE, null, ex);
          }
          System.out.println("Consumer received message:" + genericRecord);
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
          logger.log(Level.SEVERE, "Error while consuming records", ex);
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

  @Override
  public void close() {

    consume = false;
  }

}
