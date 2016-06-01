package io.hops.kafka;

/**
 *
 * @author misdess
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
        //super("KafkaConsumerExample", false);
        //Get Consumer properties
        Properties props = HopsKafkaUtil.getInstance().getConsumerConfig();      
        
        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    @Override
    public void run() {
        logger.log(Level.INFO, "**** Consumer checkpoint 0");
        consumer.subscribe(Collections.singletonList(this.topic));
        logger.log(Level.INFO, "**** Consumer checkpoint 0.1");
        while(true){
            ConsumerRecords<Integer, String> records = consumer.poll(1000);
        //synchronized(kafkaRecords){
            for (ConsumerRecord<Integer, String> record : records) {
                 Schema.Parser parser = new Schema.Parser();
                 logger.log(Level.INFO, "**** Consumer checkpoint 1");
                 Schema schema = parser.parse(hopsKafkaUtil.getSchema());
                 logger.log(Level.INFO, "**** Consumer checkpoint 2");

                 Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
                 logger.log(Level.INFO, "**** Consumer checkpoint 3");
                 logger.log(Level.INFO, "**** ConsumerRecord key:{0}",record.key());
                 logger.log(Level.INFO, "**** ConsumerRecord value:{0}",record.value());
                 GenericRecord genericRecord = recordInjection.invert(record.value().getBytes()).get();
                 logger.log(Level.INFO, "**** Consumer checkpoint 4");

                 System.out.println("str1= " + genericRecord.get("str1")
                        + ", str2= " + genericRecord.get("str2"));
                 logger.log(Level.INFO, "Received message: {0}", record.value()); 
                 System.out.println("***Received message:"+record.value());
//                 
                 //kafkaRecords.add((String) genericRecord.get("str1"));
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                Logger.getLogger(HopsKafkaConsumer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public List<String> getKafkaRecords() {
        return kafkaRecords;
    }
       
   
}