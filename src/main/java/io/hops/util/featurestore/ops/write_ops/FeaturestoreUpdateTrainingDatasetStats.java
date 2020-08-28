package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.featurestore.ops.FeaturestoreOp;

/**
 * Builder class for Update-TrainingDataset-Stats operation on the Hopsworks Featurestore
 */
public class FeaturestoreUpdateTrainingDatasetStats extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the training dataset to update stats of
   */
  public FeaturestoreUpdateTrainingDatasetStats(String name) {
    super(name);
  }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }
  
  /**
   * Updates the stats of a training dataset
   */
  public void write() {}
}
