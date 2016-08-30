package io.hops.kafkautil.flink;

import io.hops.kafkautil.HopsKafkaProducer;
import io.hops.kafkautil.HopsKafkaUtil;
import io.hops.kafkautil.SchemaNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 *
 *
 * @param <IN>
 */
public class HopsFlinkKafkaProducer<IN> extends RichSinkFunction<IN> {

  //KeyedSerializationSchema<IN> serializationSchema;
  HopsKafkaProducer hopsKafkaProducer;
  SerializationSchema<IN> serializationSchema;

  public HopsFlinkKafkaProducer(String topic,
          SerializationSchema<IN> serializationSchema) throws
          SchemaNotFoundException {
    //this.serializationSchema = new KeyedSerializationSchemaWrapper<>(serializationSchema);
    hopsKafkaProducer = HopsKafkaUtil.getInstance().getHopsKafkaProducer(topic);

  }

  @Override
  public void invoke(IN in) throws Exception {
    //Get data from Flink app and create the Avro record to produce to Kafka
//    String key = Arrays.toString(serializationSchema.serializeKey(in));
//    String value = Arrays.toString(serializationSchema.serializeValue(in));
    String data = Arrays.toString(serializationSchema.serialize(in));
    Map<String, Object> record = new HashMap<>();
    record.put("str1", data);
    hopsKafkaProducer.produce(record);

  }

}
