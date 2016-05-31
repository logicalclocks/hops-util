package io.hops.kafka;

import org.apache.avro.Schema;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author misdess
 */
public class HopsKafkaProducer extends Thread {

    private static final Logger logger = Logger.
            getLogger(HopsKafkaProducer.class.getName());

    private final KafkaProducer<Integer, byte[]> producer;
    private final String topic;
    private final Boolean isAsync;
    private final Schema schema;
    private final Injection<GenericRecord, byte[]> recordInjection;
    
    public HopsKafkaProducer(String topic, Boolean isAsync) {

        Properties props = HopsKafkaProperties.defaultProps();
        props.put("client.id", "HopsProducer");

        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(HopsKafkaUtil.getInstance().getSchema());
        recordInjection = GenericAvroCodecs.toBinary(schema);
    }

    @Override
    public void run() {
        int messageNo = 1;
        while (messageNo < 20) {
            //this is the bare text message
            String messageStr = "Message_" + messageNo;
            //create the avro message
            GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("msg", messageStr);
            byte[] bytes = recordInjection.apply(avroRecord);

            long startTime = System.currentTimeMillis();
            if (isAsync) { // Send asynchronously
                producer.send(new ProducerRecord<>(topic, messageNo, bytes),
                        new DemoCallBack(startTime, messageNo, messageStr));
            } else { // Send synchronously
                try {
                    producer.send(new ProducerRecord<>(topic, messageNo, bytes)).get();
                    logger.log(Level.SEVERE, "Sent message {0}, {1}",
                            new Object[]{messageNo, messageStr});
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;
        }
    }
}

class DemoCallBack implements Callback {
    
   private static final Logger logger = Logger.
            getLogger(DemoCallBack.class.getName());

    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

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
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        
        if (metadata != null) {
            logger.log(Level.SEVERE, "Message {0} is sent",
                    new Object[]{message});
        } else {
            exception.printStackTrace();
        }
    }
}
