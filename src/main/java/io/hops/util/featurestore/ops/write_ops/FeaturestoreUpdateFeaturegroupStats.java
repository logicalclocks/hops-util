package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.featurestore.ops.FeaturestoreOp;

/**
 * Builder class for Update-Featuregroup-Stats operation on the Hopsworks Featurestore
 */
public class FeaturestoreUpdateFeaturegroupStats extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the featuregroup to update stats of
   */
  public FeaturestoreUpdateFeaturegroupStats(String name) { super(name); }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }
  
  /**
   * Updates the stats of a featuregroup
   */
  public void write() {}
}
