/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hops.util;

import io.hops.util.exceptions.CredentialsNotFoundException;
import io.hops.util.exceptions.SchemaNotFoundException;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
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

/**
 * Serializer for Converting data into Avro records to be produced/consumed
 * by Kafka.
 * 
 */
public class AvroDeserializer implements DeserializationSchema<String>,
    SerializationSchema<Tuple4<String, String, String, String>> {

  private static final long serialVersionUID = 1L;
  private String schemaJson;
  private transient Schema.Parser parser = new Schema.Parser();
  private transient Schema schema;
  private transient Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
  private boolean initialized = false;

  /**
   *
   * @param topicName Kafka topic name
   */
  public AvroDeserializer(String topicName) {
    try {
      schemaJson = Hops.getSchema(topicName);
    } catch (SchemaNotFoundException | CredentialsNotFoundException ex) {
      Logger.getLogger(AvroDeserializer.class.getName()).log(Level.SEVERE, null,
          ex);
    }
  }

  /**
   *
   * @param message The message, as a byte array.
   * @return The deserialized message as a String object.
   * @throws IOException IOException
   */
  @Override
  public String deserialize(byte[] message) throws IOException {
    if (!initialized) {
      parser = new Schema.Parser();
      schema = parser.parse(schemaJson);
      recordInjection = GenericAvroCodecs.toBinary(schema);
      initialized = true;
    }
    GenericRecord genericRecord = recordInjection.invert(message).get();
    return genericRecord.toString().replaceAll("\\\\u001A", "");
  }

  /**
   * Method to decide whether the element signals the end of the stream. 
   * If true is returned the element won't be emitted.
   * @param t The element to test for the end-of-stream signal.
   * @return false.
   */
  @Deprecated
  @Override
  public boolean isEndOfStream(String t) {
    return false;
  }

  /**
   *
   * @return String produced type
   */
  @Override
  public TypeInformation<String> getProducedType() {
    return BasicTypeInfo.STRING_TYPE_INFO;
  }

  /**
   *
   * @param t input Tuple4 to serialize.
   * @return Kafka record as byte array.
   */
  @Override
  public byte[] serialize(Tuple4<String, String, String, String> t) {

    if (!initialized) {
      parser = new Schema.Parser();
      schema = parser.parse(schemaJson);
      recordInjection = GenericAvroCodecs.toBinary(schema);
      initialized = true;
    }
    GenericData.Record avroRecord = new GenericData.Record(schema);
    for (int i = 0; i < t.getArity() - 1; i += 2) {
      avroRecord.put(t.getField(i).toString(), t.getField(i + 1).toString());
    }

    byte[] bytes = recordInjection.apply(avroRecord);
    return bytes;
  }

}
