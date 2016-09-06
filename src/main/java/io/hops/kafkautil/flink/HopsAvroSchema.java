package io.hops.kafkautil.flink;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import io.hops.kafkautil.HopsKafkaUtil;
import io.hops.kafkautil.SchemaNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 * <p>
 */
public class HopsAvroSchema implements DeserializationSchema<String>,
        SerializationSchema<Tuple4<String, String, String, String>> {

  private static final long serialVersionUID = 1L;
  private String schemaJson;
//  transient Schema.Parser parser = new Schema.Parser();
//  transient Schema schema;
//  transient Injection<GenericRecord, byte[]> recordInjection
//          = GenericAvroCodecs.
//          toBinary(schema);

  public HopsAvroSchema(String topicName) {
    try {
      System.out.println("HopsAvroSchema.getting_schema_for_topic:" + topicName);
      schemaJson = HopsKafkaUtil.getInstance().getSchema(topicName);

    } catch (SchemaNotFoundException ex) {
      Logger.getLogger(HopsAvroSchema.class.getName()).log(Level.SEVERE, null,
              ex);
    }
  }

  @Override
  public String deserialize(byte[] bytes) throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(schemaJson);
    Injection<GenericRecord, byte[]> recordInjection
            = GenericAvroCodecs.
            toBinary(schema);
    GenericRecord genericRecord = recordInjection.invert(bytes).get();
    //Object value = genericRecord.get("str1");
    System.out.println("HopsAvroSchema.deserialize.t:" + genericRecord.
            toString());
    return genericRecord.toString().replaceAll("\\\\u001A", "");
  }

  @Override
  public boolean isEndOfStream(String t) {
    return false;
  }

  @Override
  public TypeInformation<String> getProducedType() {
    return BasicTypeInfo.STRING_TYPE_INFO;
  }

  @Override
  public byte[] serialize(Tuple4<String, String, String, String> t) {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(schemaJson);
    Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.
            toBinary(schema);

    System.out.println("HopsAvroSchema.serialize.t:" + t);
    GenericData.Record avroRecord = new GenericData.Record(schema);
    for(int i=0; i<t.getArity()-1;i+=2){
      System.out.println("HopsAvroSchema.t.getField(i):"+t.getField(i).toString());
      System.out.println("HopsAvroSchema.t.getField(i+1):"+t.getField(i+1).toString());
      avroRecord.put(t.getField(i).toString(), t.getField(i+1).toString());
    }
    
//    for (Schema.Field field : schema.getFields()) {
//      avroRecord.put(field.name(), t);
//    }

    byte[] bytes = recordInjection.apply(avroRecord);
    return bytes;
  }

}
