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

package io.hops.util.flink;

import java.util.Properties;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 * Wrapper class for FlinkKafkaProducer.
 * 
 */
public class FlinkProducer extends FlinkKafkaProducer09 {

  public FlinkProducer(String topic,
          SerializationSchema serializationSchema,
          Properties props) {
    super(topic, new KeyedSerializationSchemaWrapper<>(serializationSchema),
            props);
  
  }

}
