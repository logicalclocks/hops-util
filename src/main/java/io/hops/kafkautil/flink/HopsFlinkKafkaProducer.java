package io.hops.kafkautil.flink;

import io.hops.kafkautil.HopsKafkaProducer;
import io.hops.kafkautil.HopsKafkaUtil;
import io.hops.kafkautil.SchemaNotFoundException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 *
 *
 * @param <IN>
 */
public class HopsFlinkKafkaProducer<IN> extends RichSinkFunction<IN> {

  private static final long serialVersionUID = 1L;
  KeyedSerializationSchema<IN> serializationSchema;
  HopsKafkaProducer hopsKafkaProducer;
//  SerializationSchema<IN> serializationSchema;

  public HopsFlinkKafkaProducer(String topic,
          SerializationSchema<IN> serializationSchema) throws
          SchemaNotFoundException {
    //this.serializationSchema = new KeyedSerializationSchemaWrapper<>(serializationSchema);
    hopsKafkaProducer = HopsKafkaUtil.getInstance().getHopsKafkaProducer(topic);

  }
//  public HopsFlinkKafkaProducer(String topic,
//          SerializationSchema<IN> serializationSchema)  {
//    this.serializationSchema = new KeyedSerializationSchemaWrapper<>(serializationSchema);
//    try {
//      hopsKafkaProducer = HopsKafkaUtil.getInstance().getHopsKafkaProducer(topic);
//    } catch (SchemaNotFoundException ex) {
//      Logger.getLogger(HopsFlinkKafkaProducer.class.getName()).
//              log(Level.SEVERE, null, ex);
//    }
//
//  }

  @Override
  public void invoke(IN in) throws Exception {
    //Get data from Flink app and create the Avro record to produce to Kafka
//    String key = Arrays.toString(serializationSchema.serializeKey(in));
//    String value = Arrays.toString(serializationSchema.serializeValue(in));
    String key = Arrays.toString(serializationSchema.serializeKey(in));
    String value = Arrays.toString(serializationSchema.serializeKey(in));
    Map<String, String> record = new HashMap<>();
    System.out.println("Schema.key:" + key);
    System.out.println("Schema.value:" + value);
    record.put("str1", value);
    hopsKafkaProducer.produce(record);

  }

}
