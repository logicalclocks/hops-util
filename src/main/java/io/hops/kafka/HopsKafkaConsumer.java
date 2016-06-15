package io.hops.kafka;

/**
 * Utility class to consume messages from the Kafka service.
 * 
 */
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import java.util.ArrayList;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class HopsKafkaConsumer extends Thread {

    private static final Logger logger = Logger.getLogger(HopsKafkaConsumer.class.getName());

    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;
    private final List<String> kafkaRecords = Collections.synchronizedList(new ArrayList<String>());
    HopsKafkaUtil hopsKafkaUtil = HopsKafkaUtil.getInstance();

    public HopsKafkaConsumer(String topic) {
        
        //Get Consumer properties
        Properties props = HopsKafkaUtil.getInstance().getConsumerConfig();
        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    @Override
    public void run() {
        //Subscribe to the Kafka topic
        consumer.subscribe(Collections.singletonList(this.topic));
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(1000);
            //synchronized(kafkaRecords){
            //Get the records
            for (ConsumerRecord<Integer, String> record : records) {
                Schema.Parser parser = new Schema.Parser();
                Schema schema;
                try {
                  schema = parser.parse(hopsKafkaUtil.getSchema());
                    
                  //Convert the record using the schema
                  Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
                  GenericRecord genericRecord = recordInjection.invert(record.value().getBytes()).get();

                  System.out.println("Consumer received message:" + genericRecord);
                } catch (SchemaNotFoundException ex) {
                  logger.log(Level.SEVERE, ex.getMessage());
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                logger.log(Level.SEVERE, "Error while consuming records", ex);
            }
        }
    }

    public List<String> getKafkaRecords() {
        return kafkaRecords;
    }

}
