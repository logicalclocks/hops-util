package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.ops.FeaturestoreOp;

import javax.xml.bind.JAXBException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builder class for Read-Training Datasets operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadTrainingDatasets extends FeaturestoreOp {
  
  /**
   * Constructor
   */
  public FeaturestoreReadTrainingDatasets() {
    super("");
  }
  
  /**
   * Gets a list of all training datasets from a particular featurestore
   *
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @return a list of training datasets in the featurestore
   */
  public List<String> read() throws FeaturestoreNotFound, JAXBException {
    return Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read().
      getTrainingDatasets().stream()
      .map(td -> FeaturestoreHelper.getTableName(td.getName(), td.getVersion())).collect(Collectors.toList());
  }
  
  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
  public FeaturestoreReadTrainingDatasets setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
}
