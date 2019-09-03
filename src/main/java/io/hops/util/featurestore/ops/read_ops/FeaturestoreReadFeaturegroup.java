package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.exceptions.StorageConnectorDoesNotExistError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupType;
import io.hops.util.featurestore.dtos.featuregroup.OnDemandFeaturegroupDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreJdbcConnectorDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * Builder class for Read-Featuregroup operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadFeaturegroup extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the featuregroup to read
   */
  public FeaturestoreReadFeaturegroup(String name) {
    super(name);
  }
  
  /**
   * Gets a featuregroup from a particular featurestore
   *
   * @return a spark dataframe with the featuregroup
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   */
  public Dataset<Row> read() throws HiveNotEnabled, FeaturegroupDoesNotExistError,
      StorageConnectorDoesNotExistError {
    FeaturestoreMetadataDTO featurestoreMetadata = FeaturestoreHelper.getFeaturestoreMetadataCache();
    FeaturegroupDTO featuregroupDTO = FeaturestoreHelper.findFeaturegroup(featurestoreMetadata.getFeaturegroups(),
        name, version);
    getSpark().sparkContext().setJobGroup("Fetching Feature Group",
        "Getting Feature group: " + name + " from the featurestore:" + featurestore, true);
    if(featuregroupDTO.getFeaturegroupType() == FeaturegroupType.ON_DEMAND_FEATURE_GROUP){
      return readOnDemandFeaturegroup((OnDemandFeaturegroupDTO) featuregroupDTO, featurestoreMetadata);
    } else {
      return readCachedFeaturegroup();
    }
  }

  /**
   * Gets an on-demand featuregroup from a particular featurestore
   *
   * @param onDemandFeaturegroupDTO featuregroup metadata
   * @param featurestoreMetadataDTO featurestore metadata
   * @return a spark dataframe with the featuregroup
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   */
  public Dataset<Row> readOnDemandFeaturegroup(
      OnDemandFeaturegroupDTO onDemandFeaturegroupDTO, FeaturestoreMetadataDTO featurestoreMetadataDTO)
      throws HiveNotEnabled, StorageConnectorDoesNotExistError {
    FeaturestoreJdbcConnectorDTO jdbcConnector = (FeaturestoreJdbcConnectorDTO) FeaturestoreHelper.findStorageConnector(
        featurestoreMetadataDTO.getStorageConnectors(), onDemandFeaturegroupDTO.getJdbcConnectorName());
    
    String jdbcConnectionString = FeaturestoreHelper.getJDBCUrlFromConnector(jdbcConnector, jdbcArguments);

    // Add custom JDBC dialects
    FeaturestoreHelper.registerCustomJdbcDialects();

    // Read the onDemandFeaturegroup using Spark
    return FeaturestoreHelper.getOnDemandFeaturegroup(getSpark(), onDemandFeaturegroupDTO.getQuery(),
        jdbcConnectionString);
  }

  /**
   * Gets a cached featuregroup from a particular featurestore
   *
   * @return a spark dataframe with the featuregroup
   * @throws HiveNotEnabled HiveNotEnabled
   */
  public Dataset<Row> readCachedFeaturegroup() throws HiveNotEnabled {
    return FeaturestoreHelper.getCachedFeaturegroup(getSpark(), name, featurestore, version);
  }
  
  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
  public FeaturestoreReadFeaturegroup setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreReadFeaturegroup setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreReadFeaturegroup setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
  
  public FeaturestoreReadFeaturegroup setVersion(int version) {
    this.version = version;
    return this;
  }

  public FeaturestoreReadFeaturegroup setJdbcArguments(Map<String, String> jdbcArguments) {
    this.jdbcArguments = jdbcArguments;
    return this;
  }
  
}
