package io.hops.kafka;

import org.apache.avro.Schema;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author misdess
 */
public class HopsKafkaProducer {

    private static final Logger logger = Logger.
            getLogger(HopsKafkaProducer.class.getName());

    private final KafkaProducer<String ,byte[]> producer;
    private final String topic;
    private final Boolean isAsync;
    private final Schema schema;
    private final Injection<GenericRecord, byte[]> recordInjection;
    private final int numberOfMessages; //How many messages to produce
    public HopsKafkaProducer(String topic, Boolean isAsync, int numberOfMessages) throws SchemaNotFoundException {

        Properties props = HopsKafkaProperties.defaultProps();
        props.put("client.id", "HopsProducer");

        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
        this.numberOfMessages = numberOfMessages;
        Schema.Parser parser = new Schema.Parser();
        logger.log(Level.INFO, "Trying to get schema for topic:{0}", topic);
        String avroSchema = HopsKafkaUtil.getInstance().getSchema();
        
        logger.log(Level.INFO, "Avro schema for topic:{0}", avroSchema);
        schema = parser.parse(avroSchema);
        logger.log(Level.INFO, "Got schema:{0}", schema);
        recordInjection = GenericAvroCodecs.toBinary(schema);
    }

    //@Override
    public void run() {
        int messageNo = 0;
        while (messageNo < numberOfMessages) {
            //this is the bare text message
            String messageStr = "Message_" + messageNo;
            //create the avro message
            GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("str1", "My Message1");
            avroRecord.put("str2", "My Message2");
            byte[] bytes = recordInjection.apply(avroRecord);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, bytes);
            producer.send(record);
            try {
                Thread.sleep(250);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
           
            System.out.println("Producer sent message:"+ messageStr);
            System.out.println("Producer sent message record:"+ record);

            messageNo++;
        }
           
    }
}

/*class DemoCallBack implements Callback {
    
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
    /*public void onCompletion(RecordMetadata metadata, Exception exception) {
        
        if (metadata != null) {
            logger.log(Level.SEVERE, "Message {0} is sent",
                    new Object[]{message});
        } else {
            exception.printStackTrace();
        }
    }
}*/
