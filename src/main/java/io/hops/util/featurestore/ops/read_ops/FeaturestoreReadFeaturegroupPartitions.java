package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.exceptions.CannotReadPartitionsOfOnDemandFeaturegroups;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupType;
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
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws CannotReadPartitionsOfOnDemandFeaturegroups CannotReadPartitionsOfOnDemandFeaturegroups
   */
  public Dataset<Row> read() throws HiveNotEnabled, FeaturegroupDoesNotExistError,
      CannotReadPartitionsOfOnDemandFeaturegroups {
    FeaturestoreMetadataDTO featurestoreMetadata = FeaturestoreHelper.getFeaturestoreMetadataCache();
    FeaturegroupDTO featuregroupDTO = FeaturestoreHelper.findFeaturegroup(featurestoreMetadata.getFeaturegroups(),
        name, version);
    if(featuregroupDTO.getFeaturegroupType() == FeaturegroupType.ON_DEMAND_FEATURE_GROUP) {
      throw new CannotReadPartitionsOfOnDemandFeaturegroups(
          "Read partitions operation is only supported for cached feature groups");
    }
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
