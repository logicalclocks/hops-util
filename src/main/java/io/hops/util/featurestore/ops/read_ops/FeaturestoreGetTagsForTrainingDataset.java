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

import io.hops.util.Constants;
import io.hops.util.FeaturestoreRestClient;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.TagError;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.ops.FeaturestoreOp;

import javax.xml.bind.JAXBException;

public class FeaturestoreGetTagsForTrainingDataset extends FeaturestoreOp {

  public FeaturestoreGetTagsForTrainingDataset(String name) {
    super(name);
  }
  
  @Override
  public Object read() throws FeaturestoreNotFound, JAXBException, TrainingDatasetDoesNotExistError, TagError {

    int id = FeaturestoreHelper.getTrainingDatasetId(featurestore, name, version);

    return FeaturestoreRestClient.getTags(getName(), getFeaturestore(),
        Constants.HOPSWORKS_REST_TRAININGDATASETS_RESOURCE, id);
  }
  
  @Override
  public void write() {
    throw new UnsupportedOperationException(
        "write() is not supported on a read operation");
  }
  
  public FeaturestoreGetTagsForTrainingDataset setFeaturestore(
      String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreGetTagsForTrainingDataset setVersion(int version) {
    this.version = version;
    return this;
  }
}
