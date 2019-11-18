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
package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.FeaturestoreRestClient;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.FeaturegroupMetadataError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.featurestore.ops.FeaturestoreOp;

import javax.xml.bind.JAXBException;
import java.util.Map;

public class FeaturestoreAddMetadataToFeaturegroup extends FeaturestoreOp {
  
  private Map<String, String> metadata;
  
  public FeaturestoreAddMetadataToFeaturegroup(String name) {
    super(name);
  }
  
  @Override
  public Object read() {
    throw new UnsupportedOperationException(
        "read() is not supported on a write operation");
  }
  
  @Override
  public void write() throws JAXBException,
      FeaturestoreNotFound, FeaturegroupDoesNotExistError,
      FeaturegroupMetadataError {
    if (metadata == null || metadata.isEmpty()) {
      throw new IllegalArgumentException("No metadata is provided.");
    }
    
    for (Map.Entry<String, String> e : metadata.entrySet()) {
      FeaturestoreRestClient.addMetadata(getName(), getFeaturestore(),
          getVersion(), e.getKey(), e.getValue());
    }
  }
  
  public FeaturestoreAddMetadataToFeaturegroup setFeaturestore(
      String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreAddMetadataToFeaturegroup setVersion(int version) {
    this.version = version;
    return this;
  }
  
  public FeaturestoreAddMetadataToFeaturegroup setMetadata(Map<String,
      String> metadata) {
    this.metadata = metadata;
    return this;
  }
  
}
