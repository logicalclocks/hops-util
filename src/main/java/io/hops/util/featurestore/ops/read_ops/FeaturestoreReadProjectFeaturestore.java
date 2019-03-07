package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.ops.FeaturestoreOp;

/**
 * Builder class for Read-ProjectFeaturestore operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadProjectFeaturestore extends FeaturestoreOp {
  
  /**
   * Constructor
   */
  public FeaturestoreReadProjectFeaturestore() {
    super("");
  }
  
  /**
   * Gets the project's featurestore name (project_featurestore)
   *
   * @return the featurestore name (hive db)
   */
  public String read() {
    return FeaturestoreHelper.getProjectFeaturestore();
  }
  
  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
}
