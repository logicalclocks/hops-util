package io.hops.kafkautil.flink;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import io.hops.kafkautil.HopsKafkaProperties;
import io.hops.kafkautil.HopsKafkaUtil;
import io.hops.kafkautil.SchemaNotFoundException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 *
 * @param <IN>
 */
public class HopsFlinkKafkaProducer<IN> extends FlinkKafkaProducerBase<IN> {

  private static final long serialVersionUID = 1L;
//  private final KafkaProducer<String, byte[]> producer = null;
//  private transient Injection<GenericRecord, byte[]> recordInjection = null;
//  private transient GenericData.Record avroRecord = null;
  private transient ProducerRecord<byte[], byte[]> record = null;
//  private transient Schema topicSchema = null;
//  private transient Schema.Parser parser = null;
//  private String schemaJson;
//  private boolean initialized;

  public HopsFlinkKafkaProducer(String topic,
          SerializationSchema<IN> serializationSchema,
          KafkaPartitioner<IN> customPartitioner) {
    super(topic, new KeyedSerializationSchemaWrapper<>(serializationSchema),
            HopsKafkaProperties.defaultProps(),
            customPartitioner);
//    try {
//      System.out.println("kafka.hopsutil.schema_topic:" + topic);
//      schemaJson = HopsKafkaUtil.getInstance().getSchema(topic);
//      System.out.println("kafka.hopsutil.schema_json:" + schemaJson);
//
//    } catch (SchemaNotFoundException ex) {
//      Logger.getLogger(HopsFlinkKafkaProducer.class.getName()).
//              log(Level.SEVERE, null, ex);
//    }
  }

  @Override
  public void invoke(IN in) throws Exception {
    //Get data from Flink app and create the Avro record to produce to Kafka

//    if (!initialized) {
//      parser = new Schema.Parser();
//      topicSchema = parser.parse(schemaJson);
//      recordInjection = GenericAvroCodecs.toBinary(topicSchema);
//      initialized = true;
//    }
    byte[] bytes = schema.serializeValue(in);
//    System.out.println("Schema.value:" + value);
//    avroRecord = new GenericData.Record(topicSchema);
//    for(Field field : topicSchema.getFields()){
//      avroRecord.put(field.name(), value);
//    }
//
//    byte[] bytes = recordInjection.apply(avroRecord);
    record = new ProducerRecord<>(topicId, bytes);
    producer.send(record);
  }

  
}
