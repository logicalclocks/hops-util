package io.hops.kafka;

/**
 *
 * @author misdess
 */

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

public class HopsKafkaConsumer extends ShutdownableThread {
    private static final Logger logger = Logger.getLogger(HopsKafkaConsumer.class.getName());
    
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;
    private final List<ConsumerRecord> kafkaRecords = Collections.synchronizedList(new ArrayList<ConsumerRecord>()); 
    
    public HopsKafkaConsumer(String topic) {
        super("KafkaConsumerExample", false);
        //Get Consumer properties
        Properties props = HopsKafkaUtil.getConsumerConfig();        
        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    @Override
    public void doWork() {
        consumer.subscribe(Collections.singletonList(this.topic));
        ConsumerRecords<Integer, String> records = consumer.poll(1000);
        synchronized(kafkaRecords){
            for (ConsumerRecord<Integer, String> record : records) {
                 kafkaRecords.add(record);
                 logger.log(Level.INFO, "Received message: {0}", record.value()); 
            }
        }
    }

    public List<ConsumerRecord> getKafkaRecords() {
        return kafkaRecords;
    }

        
    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterruptible() {
        return false;
    }
}