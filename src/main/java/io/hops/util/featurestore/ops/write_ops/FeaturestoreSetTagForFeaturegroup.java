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

import io.hops.util.Constants;
import io.hops.util.FeaturestoreRestClient;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.TagError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.ops.FeaturestoreOp;

import javax.xml.bind.JAXBException;

public class FeaturestoreSetTagForFeaturegroup extends FeaturestoreOp {
  
  public FeaturestoreSetTagForFeaturegroup(String name) {
    super(name);
  }
  
  @Override
  public Object read() {
    throw new UnsupportedOperationException(
        "read() is not supported on a write operation");
  }
  
  @Override
  public void write() throws JAXBException,
      FeaturestoreNotFound, FeaturegroupDoesNotExistError, TagError {

    int id = FeaturestoreHelper.getFeaturegroupId(featurestore, name, version);

    FeaturestoreRestClient.addTag(getName(), getFeaturestore(), getTag(), getValue(),
        Constants.HOPSWORKS_REST_FEATUREGROUPS_RESOURCE, id);
  }
  
  public FeaturestoreSetTagForFeaturegroup setFeaturestore(
      String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreSetTagForFeaturegroup setVersion(int version) {
    this.version = version;
    return this;
  }

  public FeaturestoreSetTagForFeaturegroup setTag(String tag) {
    this.tag = tag;
    return this;
  }

  public FeaturestoreSetTagForFeaturegroup setValue(String value) {
    this.value = value;
    return this;
  }

}
