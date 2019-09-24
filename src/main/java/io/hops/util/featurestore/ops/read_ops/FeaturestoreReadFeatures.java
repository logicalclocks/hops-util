package io.hops.util.featurestore.ops.read_ops;

import com.google.common.base.Strings;
import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.exceptions.OnlineFeaturestorePasswordNotFound;
import io.hops.util.exceptions.OnlineFeaturestoreUserNotFound;
import io.hops.util.exceptions.StorageConnectorDoesNotExistError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.bind.JAXBException;
import java.util.List;
import java.util.Map;

/**
 * Builder class for Read-Features operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadFeatures extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param features list of feature names to read
   */
  public FeaturestoreReadFeatures(List<String> features) {
    super(features);
  }
  
  /**
   * Gets a feature from a featurestore and a specific featuregroup.
   *
   * @return a spark dataframe with the feature
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   */
  public Dataset<Row> read() throws FeaturestoreNotFound, JAXBException, HiveNotEnabled, FeaturegroupDoesNotExistError,
    StorageConnectorDoesNotExistError, OnlineFeaturestoreUserNotFound, OnlineFeaturestorePasswordNotFound {
    if(features.isEmpty()){
      throw new IllegalArgumentException("Feature List Cannot be Empty");
    }
    try {
      return doGetFeatures();
    } catch (Exception e){
      Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
      return doGetFeatures();
    }
  }
  
  /**
   * Gets a list of features from the featurestore
   *
   * @return A spark dataframe with the features
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   */
  private Dataset<Row> doGetFeatures() throws FeaturestoreNotFound, JAXBException, HiveNotEnabled,
    FeaturegroupDoesNotExistError, StorageConnectorDoesNotExistError, OnlineFeaturestoreUserNotFound,
    OnlineFeaturestorePasswordNotFound {
    if(!Strings.isNullOrEmpty(joinKey) && featuregroupsAndVersions != null){
      return FeaturestoreHelper.getFeatures(getSpark(), features, featurestore, featuregroupsAndVersions, joinKey,
          Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read().getFeaturegroups(),
          onDemandFeaturegroupsjdbcArguments, online);
    }
    if(!Strings.isNullOrEmpty(joinKey) && featuregroupsAndVersions == null){
      return doGetFeatures(getSpark(), features, featurestore,
        Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read(), joinKey);
    }
    if(Strings.isNullOrEmpty(joinKey) && featuregroupsAndVersions != null){
      return doGetFeatures(getSpark(), features, featurestore, Hops.getFeaturestoreMetadata()
          .setFeaturestore(featurestore).read(), featuregroupsAndVersions);
    }
    return doGetFeatures(getSpark(), features, featurestore, Hops.getFeaturestoreMetadata()
      .setFeaturestore(featurestore).read());
  }
  
  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe. This method is used if the user
   * has itself provided a set of featuregroups where the features are located and should be queried from
   * but not a join key, it does not infer the featuregroups but infers the join key
   *
   * @param sparkSession             the spark session
   * @param features                 the list of features to get
   * @param featurestore             the featurestore to query
   * @param featurestoreMetadata     metadata of the featurestore to query
   * @param featuregroupsAndVersions a map of (featuregroup to version) where the featuregroups are located
   * @return a spark dataframe with the features
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   */
  private Dataset<Row> doGetFeatures(
    SparkSession sparkSession, List<String> features, String featurestore,
    FeaturestoreMetadataDTO featurestoreMetadata,
    Map<String, Integer> featuregroupsAndVersions) throws FeaturegroupDoesNotExistError, HiveNotEnabled,
    StorageConnectorDoesNotExistError, OnlineFeaturestorePasswordNotFound, FeaturestoreNotFound,
    OnlineFeaturestoreUserNotFound, JAXBException {
    List<FeaturegroupDTO> featuregroupsMetadata = featurestoreMetadata.getFeaturegroups();
    List<FeaturegroupDTO> filteredFeaturegroupsMetadata =
      FeaturestoreHelper.filterFeaturegroupsBasedOnMap(featuregroupsAndVersions, featuregroupsMetadata);
    String joinKey = FeaturestoreHelper.getJoinColumn(filteredFeaturegroupsMetadata);
    return FeaturestoreHelper.getFeatures(sparkSession, features, featurestore, featuregroupsAndVersions, joinKey,
        filteredFeaturegroupsMetadata, onDemandFeaturegroupsjdbcArguments, online);
  }
  
  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe. This method will infer
   * in which featuregroups the features belong but uses a user-supplied join key
   *
   * @param sparkSession the spark session
   * @param features     the list of features to get
   * @param featurestore the featurestore to query
   * @param featurestoreMetadata metadata of the featurestore to query
   * @param joinKey      the key to join on
   * @return a spark dataframe with the features
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   */
  private Dataset<Row> doGetFeatures(
    SparkSession sparkSession, List<String> features,
    String featurestore, FeaturestoreMetadataDTO featurestoreMetadata, String joinKey)
    throws FeaturegroupDoesNotExistError, HiveNotEnabled, StorageConnectorDoesNotExistError,
    OnlineFeaturestorePasswordNotFound, FeaturestoreNotFound, OnlineFeaturestoreUserNotFound, JAXBException {
    List<FeaturegroupDTO> featuregroupsMetadata = featurestoreMetadata.getFeaturegroups();
    return FeaturestoreHelper.getFeatures(sparkSession, features, featurestore, featuregroupsMetadata, joinKey,
        onDemandFeaturegroupsjdbcArguments, online);
  }
  
  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe. This method will infer
   * in which featuregroups the features belong and which join_key to use using metadata from the metastore
   *
   * @param sparkSession the spark session
   * @param features     the list of features to get
   * @param featurestore the featurestore to query
   * @param featurestoreMetadata metadata of which features exist in the featurestore, used to infer how to join
   *                             features together
   * @return a spark dataframe with the features
   * * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   */
  private Dataset<Row> doGetFeatures(
    SparkSession sparkSession, List<String> features,
    String featurestore, FeaturestoreMetadataDTO featurestoreMetadata)
    throws FeaturegroupDoesNotExistError, HiveNotEnabled, StorageConnectorDoesNotExistError,
    OnlineFeaturestorePasswordNotFound, FeaturestoreNotFound, OnlineFeaturestoreUserNotFound, JAXBException {
    List<FeaturegroupDTO> featuregroupsMetadata = featurestoreMetadata.getFeaturegroups();
    List<FeaturegroupDTO> featuregroupsMatching =
      FeaturestoreHelper.findFeaturegroupsThatContainsFeatures(featuregroupsMetadata, features, featurestore);
    String joinKey = FeaturestoreHelper.getJoinColumn(featuregroupsMatching);
    return FeaturestoreHelper.getFeatures(sparkSession, features, featurestore, featuregroupsMatching, joinKey,
        onDemandFeaturegroupsjdbcArguments, online);
  }
  
  public FeaturestoreReadFeatures setFeatures(List<String> features) {
    this.features = features;
    return this;
  }
  
  public FeaturestoreReadFeatures setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
  
  public FeaturestoreReadFeatures setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreReadFeatures setFeaturegroupsAndVersions(Map<String, Integer> featuregroupsAndVersions) {
    this.featuregroupsAndVersions = featuregroupsAndVersions;
    return this;
  }
  
  public FeaturestoreReadFeatures setJoinKey(String joinKey) {
    this.joinKey = joinKey;
    return this;
  }

  public FeaturestoreReadFeatures setOnDemandFeaturegroupsjdbcArguments(Map<String, Map<String, String>>
                                                                            onDemandFeaturegroupsjdbcArguments) {
    this.onDemandFeaturegroupsjdbcArguments = onDemandFeaturegroupsjdbcArguments;
    return this;
  }
  
  public FeaturestoreReadFeatures setOnline(Boolean online) {
    this.online = online;
    return this;
  }
  
}
