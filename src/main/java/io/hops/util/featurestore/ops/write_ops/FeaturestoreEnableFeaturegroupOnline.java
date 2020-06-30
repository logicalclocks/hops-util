package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.FeaturestoreRestClient;
import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.FeaturegroupEnableOnlineError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.OnlineFeaturestoreNotEnabled;
import io.hops.util.exceptions.OnlineFeaturestorePasswordNotFound;
import io.hops.util.exceptions.OnlineFeaturestoreUserNotFound;
import io.hops.util.exceptions.StorageConnectorDoesNotExistError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.feature.FeatureDTO;
import io.hops.util.featurestore.dtos.featuregroup.CachedFeaturegroupDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.dtos.featuregroup.OnDemandFeaturegroupDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadFeaturegroup;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.bind.JAXBException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builder class for Enable Online Serving for a Featuregroup operation on the Hopsworks Featurestore.
 */
public class FeaturestoreEnableFeaturegroupOnline extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the feature group to enable online serving for
   */
  public FeaturestoreEnableFeaturegroupOnline(String name) {
    super(name);
  }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }
  
  /**
   * Enables online feature serving for a feature group
   *
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws FeaturegroupEnableOnlineError FeaturegroupEnableOnlineError
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  public void write()
    throws JAXBException, FeaturestoreNotFound, JWTNotFoundException, FeaturegroupDoesNotExistError,
    FeaturegroupEnableOnlineError, HiveNotEnabled, OnlineFeaturestoreUserNotFound, OnlineFeaturestorePasswordNotFound,
    StorageConnectorDoesNotExistError, OnlineFeaturestoreNotEnabled {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    FeaturestoreMetadataDTO featurestoreMetadata = FeaturestoreHelper.getFeaturestoreMetadataCache();
    FeaturegroupDTO featuregroupDTO = FeaturestoreHelper.findFeaturegroup(featurestoreMetadata.getFeaturegroups(),
      name, version);
    if(featuregroupDTO instanceof OnDemandFeaturegroupDTO) {
      throw new IllegalArgumentException("Cannot Enable Online Feature Serving on an on-demand " +
        "feature group, this operation is only supported for cached feature groups");
    }
    featuregroupDTO = setupMySQLDataTypes((CachedFeaturegroupDTO) featuregroupDTO, featurestore, getSpark());
    FeaturestoreRestClient.enableFeaturegroupOnlineRest(featuregroupDTO,
      FeaturestoreHelper.getFeaturegroupDtoTypeStr(featurestoreMetadata.getSettings(), false));
    //Update metadata cache since we modified feature group
    Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
  }
  
  /**
   * Sets up MySQL data types for the MySQL table (types are inferred from spark dataframe, or specified through the
   * "onlineTypes" argument)
   *
   * @param cachedFeaturegroupDTO dto with metadata about the featuregroup
   * @param featurestore          the featurestore to query
   * @param sparkSession          the sparksession
   * @return DTO with the MySQL types specified
   * @throws JAXBException
   * @throws FeaturestoreNotFound
   * @throws OnlineFeaturestorePasswordNotFound
   * @throws StorageConnectorDoesNotExistError
   * @throws OnlineFeaturestoreUserNotFound
   * @throws FeaturegroupDoesNotExistError
   * @throws HiveNotEnabled
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  private CachedFeaturegroupDTO setupMySQLDataTypes(CachedFeaturegroupDTO cachedFeaturegroupDTO, String featurestore,
    SparkSession sparkSession)
    throws JAXBException, FeaturestoreNotFound, OnlineFeaturestorePasswordNotFound, StorageConnectorDoesNotExistError,
    OnlineFeaturestoreUserNotFound, FeaturegroupDoesNotExistError, HiveNotEnabled, OnlineFeaturestoreNotEnabled {
    Dataset<Row> sparkDf =
      new FeaturestoreReadFeaturegroup(cachedFeaturegroupDTO.getName())
         .setVersion(cachedFeaturegroupDTO.getVersion())
         .setFeaturestore(featurestore)
         .setSpark(sparkSession).read();
    List<String> primaryKey = new ArrayList<>();
    for (FeatureDTO feature: cachedFeaturegroupDTO.getFeatures()) {
      if(feature.getPrimary()){
        primaryKey.add(feature.getName());
      }
    }
    List<FeatureDTO> featuresSchema = FeaturestoreHelper.parseSparkFeaturesSchema(sparkDf.schema(), primaryKey,
      new ArrayList<>(), true, onlineTypes);
    cachedFeaturegroupDTO.setFeatures(featuresSchema);
    return cachedFeaturegroupDTO;
  }
  
  
  public FeaturestoreEnableFeaturegroupOnline setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreEnableFeaturegroupOnline setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreEnableFeaturegroupOnline setVersion(int version) {
    this.version = version;
    return this;
  }
  
  public FeaturestoreEnableFeaturegroupOnline setOnlineTypes(Map<String, String> onlineTypes) {
    this.onlineTypes = onlineTypes;
    return this;
  }
  
  public FeaturestoreEnableFeaturegroupOnline setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
}
