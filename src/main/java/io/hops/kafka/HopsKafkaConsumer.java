package io.hops.kafka;

/**
 *
 * @author misdess
 */

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import java.util.ArrayList;
import kafka.utils.ShutdownableThread;
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
        //super("KafkaConsumerExample", false);
        //Get Consumer properties
        Properties props = HopsKafkaUtil.getInstance().getConsumerConfig();        
        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(this.topic));
        ConsumerRecords<Integer, String> records = consumer.poll(1000);
        synchronized(kafkaRecords){
            for (ConsumerRecord<Integer, String> record : records) {
                 Schema.Parser parser = new Schema.Parser();
                 Schema schema = parser.parse(hopsKafkaUtil.getSchema());
                 Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
                 GenericRecord genericRecord = recordInjection.invert(record.value().getBytes()).get();
                 kafkaRecords.add((String) genericRecord.get("msg"));
                 logger.log(Level.INFO, "Received message: {0}", record.value()); 
            }
        }
    }

    public List<String> getKafkaRecords() {
        return kafkaRecords;
    }
       
   
}