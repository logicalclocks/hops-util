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
package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.FeaturestoreRestClient;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.FeaturegroupMetadataError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.featurestore.ops.FeaturestoreOp;

import javax.xml.bind.JAXBException;
import java.util.HashMap;
import java.util.Map;

public class FeaturestoreGetMetadataForFeaturegroup extends FeaturestoreOp {
  
  private String[] keys;
  
  public FeaturestoreGetMetadataForFeaturegroup(String name) {
    super(name);
  }
  
  @Override
  public Object read() throws FeaturestoreNotFound, JAXBException,
      FeaturegroupDoesNotExistError, FeaturegroupMetadataError {
    if (keys == null || keys.length == 0) {
      return FeaturestoreRestClient.getMetadata(getName(), getFeaturestore(),
          getVersion(), null);
    } else {
      Map<String, String> results = new HashMap<>();
      for (String key : keys) {
        results.putAll(FeaturestoreRestClient.getMetadata(getName(),
            getFeaturestore(), getVersion(), key));
      }
      return results;
    }
  }
  
  @Override
  public void write() {
    throw new UnsupportedOperationException(
        "write() is not supported on a read operation");
  }
  
  public FeaturestoreGetMetadataForFeaturegroup setFeaturestore(
      String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreGetMetadataForFeaturegroup setVersion(int version) {
    this.version = version;
    return this;
  }
  
  public FeaturestoreGetMetadataForFeaturegroup setKeys(String... keys) {
    this.keys = keys;
    return this;
  }
}
