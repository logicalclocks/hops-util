package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Builder class for Read-Featuregroup operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadFeaturegroupPartitions extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the featuregroup to read
   */
  public FeaturestoreReadFeaturegroupPartitions(String name) {
    super(name);
  }
  
  /**
   * Gets a featuregroup from a particular featurestore
   *
   * @return a spark dataframe with the featuregroup
   * @throws HiveNotEnabled HiveNotEnabled
   */
  public Dataset<Row> read() throws HiveNotEnabled {
    
    return FeaturestoreHelper.getFeaturegroupPartitions(getSpark(), name, featurestore, version);
  }
  
  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
  public FeaturestoreReadFeaturegroupPartitions setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreReadFeaturegroupPartitions setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreReadFeaturegroupPartitions setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
  
  public FeaturestoreReadFeaturegroupPartitions setVersion(int version) {
    this.version = version;
    return this;
  }
  
}
