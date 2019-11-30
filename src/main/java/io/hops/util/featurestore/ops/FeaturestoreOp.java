package io.hops.util.featurestore.ops;

import io.hops.util.Hops;
import io.hops.util.exceptions.CannotInsertIntoOnDemandFeaturegroups;
import io.hops.util.exceptions.CannotReadPartitionsOfOnDemandFeaturegroups;
import io.hops.util.exceptions.CannotUpdateStatsOfOnDemandFeaturegroups;
import io.hops.util.exceptions.CannotWriteImageDataFrameException;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturegroupCreationError;
import io.hops.util.exceptions.FeaturegroupDeletionError;
import io.hops.util.exceptions.FeaturegroupDisableOnlineError;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.FeaturegroupEnableOnlineError;
import io.hops.util.exceptions.FeaturegroupUpdateStatsError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.FeaturestoresNotFound;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.exceptions.InvalidPrimaryKeyForFeaturegroup;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.OnlineFeaturestoreNotEnabled;
import io.hops.util.exceptions.OnlineFeaturestorePasswordNotFound;
import io.hops.util.exceptions.OnlineFeaturestoreUserNotFound;
import io.hops.util.exceptions.SparkDataTypeNotRecognizedError;
import io.hops.util.exceptions.StorageConnectorDoesNotExistError;
import io.hops.util.exceptions.StorageConnectorNotFound;
import io.hops.util.exceptions.StorageConnectorTypeNotSupportedForFeatureImport;
import io.hops.util.exceptions.TrainingDatasetCreationError;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetFormatNotSupportedError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.stats.StatisticsDTO;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract feature store operation, uses builder-pattern for concrete operation implementations
 */
public abstract class FeaturestoreOp {

  protected String name;
  protected SparkSession spark = null;
  protected String featurestore = FeaturestoreHelper.featurestoreGetOrDefault(null);
  protected int version = 1;
  protected String featuregroup;
  protected List<String> features;
  protected Map<String, Integer> featuregroupsAndVersions;
  protected String joinKey;
  protected String corrMethod = FeaturestoreHelper.correlationMethodGetOrDefault(null);
  protected int numBins = FeaturestoreHelper.numBinsGetOrDefault(null);
  protected int numClusters = FeaturestoreHelper.numClustersGetOrDefault(null);
  protected String mode;
  protected Dataset<Row> dataframe;
  protected Boolean descriptiveStats = true;
  protected Boolean featureCorr = true;
  protected Boolean featureHistograms = true;
  protected Boolean clusterAnalysis = true;
  protected List<String> statColumns = null;
  protected List<String> jobs = new ArrayList<>();
  protected List<String> primaryKey = new ArrayList<>();
  protected String description = "";
  protected String dataFormat = FeaturestoreHelper.dataFormatGetOrDefault(null);
  protected List<String> partitionBy = new ArrayList<>();
  protected Boolean onDemand = false;
  protected String jdbcConnector = null;
  protected String sqlQuery = "";
  protected Map<String, String> jdbcArguments;
  protected Map<String, Map<String, String>> onDemandFeaturegroupsjdbcArguments;
  protected StatisticsDTO statisticsDTO;
  protected Boolean hudi = false;
  protected Map<String, String> hudiArgs = new HashMap<>();
  protected String hudiBasePath = "";
  protected String storageConnector = null;
  protected String externalPath;
  protected Boolean online = false;
  protected Boolean offline = true;
  protected Map<String, String> onlineTypes;


  /**
   * Class constructor
   *
   * @param name name of featuregroup/feature/training dataset that the operation concerns. Empty string if the
   *             the operation is applied to the entire featurestore.
   */
  public FeaturestoreOp(String name) {
    this.name = name;
  }

  /**
   * Class constructor
   *
   * @param features list of features that the operation concerns, empty list or null if it does not concert any
   *                 list of features
   */
  public FeaturestoreOp(List<String> features) {
    this.features = features;
  }


  /**
   *
   * @return name of featuregroup/feature/training dataset that the operation concerns. Empty string if the
   *         the operation is applied to the entire featurestore.
   */
  public String getName() {
    return name;
  }

  /**
   * @return spark session to use for the operation
   * @throws HiveNotEnabled HiveNotEnabled
   */
  public SparkSession getSpark() throws HiveNotEnabled {
    if(spark == null){
      spark = Hops.findSpark();
      FeaturestoreHelper.verifyHiveEnabled(spark);
    }
    return spark;
  }

  /**
   *
   * @return featurestore to apply the operation on
   */
  public String getFeaturestore() {
    return featurestore;
  }

  /**
   *
   * @return version of the featuregroup/training dataset that the operation concerns
   */
  public int getVersion() {
    return version;
  }

  /**
   * @return The featuregroup used for queries of features
   */
  public String getFeaturegroup() {
    return featuregroup;
  }

  /**
   * @return List of features to use in the query
   */
  public List<String> getFeatures() {
    return features;
  }

  /**
   * @return a map with featuregroups and their versions
   */
  public Map<String, Integer> getFeaturegroupsAndVersions() {
    return featuregroupsAndVersions;
  }

  /**
   * @return the join key to use for the query
   */
  public String getJoinKey() {
    return joinKey;
  }

  /**
   * @return the correlation method to use for feature correlation analysis (e.g pearson or spearman)
   */
  public String getCorrMethod() {
    return corrMethod;
  }

  /**
   * @return the number of bins to use for histogram statistics
   */
  public int getNumBins() {
    return numBins;
  }

  /**
   * @return the number of clusters to use for k-means clustering analysis
   */
  public int getNumClusters() {
    return numClusters;
  }

  /**
   * @return the write mode (append or overwrite)
   */
  public String getMode() {
    return mode;
  }

  /**
   * @return the dataframe to use for write operations
   */
  public Dataset<Row> getDataframe() {
    return dataframe;
  }

  /**
   * @return boolean flag indicating whether descriptive stats should be computed for statistics update
   */
  public Boolean getDescriptiveStats() {
    return descriptiveStats;
  }

  /**
   * @return boolean flag indicating whether feature correlation should be computed for statistics update
   */
  public Boolean getFeatureCorr() {
    return featureCorr;
  }

  /**
   * @return boolean flag indicating whether feature histograms should be computed for statistics update
   */
  public Boolean getFeatureHistograms() {
    return featureHistograms;
  }

  /**
   * @return boolean flag indicating whether cluster analysis should be computed for statistics update
   */
  public Boolean getClusterAnalysis() {
    return clusterAnalysis;
  }

  /**
   * @return a list of columns to compute statistics for (defaults to all columns that are numeric)
   */
  public List<String> getStatColumns() {
    return statColumns;
  }

  /**
   * @return list of jobs linked to a feature group or training dataset
   */
  public List<String> getJobs() {
    return jobs;
  }

  /**
   * @return the primary key of the new featuregroup, if not specified, the first column in the
   * dataframe will be used as primary
   */
  public List<String> getPrimaryKey() {
    return primaryKey;
  }

  /**
   * @return a description of the featuregroup/training dataset
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return the format of the materialized training dataset
   */
  public String getDataFormat() {
    return dataFormat;
  }

  /**
   * @return the columns to partition feature group by
   */
  public List<String> getPartitionBy() {
    return partitionBy;
  }

  /**
   * @return whether it is an on-demand feature group
   */
  public Boolean getOnDemand() {
    return onDemand;
  }

  /**
   * @return SQL query for on-demand feature groups
   */
  public String getSqlQuery() {
    return sqlQuery;
  }

  /**
   * @return arguments for the JDBC connection string to be substituted at runtime
   */
  public Map<String, String> getJdbcArguments() {
    return jdbcArguments;
  }

  /**
   * @return sink for saving a training dataset, if not specified it will default to the Training Datasets folder in
   *         HopsFS but you can also specify an S3 bucket
   */
  public String getSink() {
    return storageConnector;
  }

  /**
   * @return jdbc connector for on-demand feature groups
   */
  public String getJdbcConnector() {
    return jdbcConnector;
  }

  /**
   * @return arguments for the JDBC connection strings for getting features from multiple feature groups.
   */
  public Map<String, Map<String, String>> getOnDemandFeaturegroupsjdbcArguments() {
    return onDemandFeaturegroupsjdbcArguments;
  }

  /**
   * @return statistics provided by the user to set for a hive table to sync with the feature store
   */
  public StatisticsDTO getStatisticsDTO() {
    return statisticsDTO;
  }

  /**
   * @return boolean flag whether the operation concerns a Hudi dataset or not
   */
  public Boolean getHudi() {
    return hudi;
  }

  /**
   * @return hudi arguments provided by the user
   */
  public Map<String, String> getHudiArgs() {
    return hudiArgs;
  }

  /**
   * @return the base path to the hudi dataset
   */
  public String getHudiBasePath() {
    return hudiBasePath;
  }

  /**
   * @return storage connector for importing external feature groups into Hopsworks Feature Store
   */
  public String getStorageConnector() {
    return storageConnector;
  }

  /**
   * @return path for external storage systems (e.g for importing an external feature group into the Hopsworks
   * feature store
   */
  public String getExternalPath() {
    return externalPath;
  }
  
  /**
   * @return whether to apply the operation to the online featurestore
   */
  public Boolean getOnline() {
    return online;
  }
  
  /**
   * @return whether to apply the operation to the offline featurestore
   */
  public Boolean getOffline() {
    return offline;
  }
  
  /**
   * @return map of (featureName to type). By default, spark datatypes will be used to infer MySQL datatypes when
   * creating MySQL tables in the Online Feature Store, but this behaviour can be overridden by providing explicit
   * feature types through this map.
   */
  public Map<String, String> getOnlineTypes() {
    return onlineTypes;
  }
  
  /**
   * Abstract read method, implemented by sub-classes for different feature store read-operations
   * This method is called by the user after populating parameters of the operation
   *
   * @return An object with results(typically a dataframe or list)
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws IOException IOException
   * @throws FeaturestoresNotFound FeaturestoresNotFound
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws StorageConnectorNotFound StorageConnectorNotFound
   * @throws CannotReadPartitionsOfOnDemandFeaturegroups CannotReadPartitionsOfOnDemandFeaturegroups
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  public abstract Object read()
    throws FeaturestoreNotFound, JAXBException, TrainingDatasetFormatNotSupportedError,
    TrainingDatasetDoesNotExistError, IOException, FeaturestoresNotFound, JWTNotFoundException, HiveNotEnabled,
    StorageConnectorDoesNotExistError, FeaturegroupDoesNotExistError, CannotReadPartitionsOfOnDemandFeaturegroups,
    StorageConnectorNotFound, OnlineFeaturestorePasswordNotFound, OnlineFeaturestoreUserNotFound,
    OnlineFeaturestoreNotEnabled;

  /**
   * Abstract write operation, implemented by sub-classes for different feature store write-operations.
   * This method is called by the user after populating parameters of the operation
   *
   * @throws FeaturegroupDeletionError FeaturegroupDeletionError
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws IOException IOException
   * @throws InvalidPrimaryKeyForFeaturegroup InvalidPrimaryKeyForFeaturegroup
   * @throws FeaturegroupCreationError FeaturegroupCreationError
   * @throws TrainingDatasetCreationError TrainingDatasetCreationError
   * @throws CannotWriteImageDataFrameException CannotWriteImageDataFrameException
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws CannotInsertIntoOnDemandFeaturegroups CannotInsertIntoOnDemandFeaturegroups
   * @throws CannotUpdateStatsOfOnDemandFeaturegroups CannotUpdateStatsOfOnDemandFeaturegroups
   * @throws StorageConnectorTypeNotSupportedForFeatureImport StorageConnectorTypeNotSupportedForFeatureImport
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws FeaturegroupEnableOnlineError FeaturegroupEnableOnlineError
   * @throws FeaturegroupDisableOnlineError FeaturegroupDisableOnlineError
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  public abstract void write()
    throws FeaturegroupDeletionError, DataframeIsEmpty, SparkDataTypeNotRecognizedError,
    JAXBException, FeaturegroupUpdateStatsError, FeaturestoreNotFound, TrainingDatasetDoesNotExistError,
    TrainingDatasetFormatNotSupportedError, IOException, InvalidPrimaryKeyForFeaturegroup, FeaturegroupCreationError,
    TrainingDatasetCreationError, CannotWriteImageDataFrameException, JWTNotFoundException,
    FeaturegroupDoesNotExistError, HiveNotEnabled, StorageConnectorDoesNotExistError,
    CannotInsertIntoOnDemandFeaturegroups, CannotUpdateStatsOfOnDemandFeaturegroups,
    StorageConnectorTypeNotSupportedForFeatureImport, OnlineFeaturestoreUserNotFound,
    OnlineFeaturestorePasswordNotFound, FeaturegroupEnableOnlineError, FeaturegroupDisableOnlineError,
    OnlineFeaturestoreNotEnabled;
}
