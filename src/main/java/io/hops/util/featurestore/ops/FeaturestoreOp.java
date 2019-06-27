package io.hops.util.featurestore.ops;

import io.hops.util.Hops;
import io.hops.util.exceptions.CannotWriteImageDataFrameException;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturegroupCreationError;
import io.hops.util.exceptions.FeaturegroupDeletionError;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.FeaturegroupUpdateStatsError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.FeaturestoresNotFound;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.exceptions.InvalidPrimaryKeyForFeaturegroup;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.SparkDataTypeNotRecognizedError;
import io.hops.util.exceptions.TrainingDatasetCreationError;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetFormatNotSupportedError;
import io.hops.util.featurestore.FeaturestoreHelper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.ArrayList;
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
  protected String jobName = FeaturestoreHelper.jobNameGetOrDefault(null);
  protected String primaryKey;
  protected String description = "";
  protected String dataFormat = FeaturestoreHelper.dataFormatGetOrDefault(null);
  protected List<String> partitionBy = new ArrayList<>();
  
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
   * @return name of the job to compute the feature group or training dataset
   */
  public String getJobName() {
    return jobName;
  }
  
  /**
   * @return the primary key of the new featuregroup, if not specified, the first column in the
   * dataframe will be used as primary
   */
  public String getPrimaryKey() {
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
   */
  public abstract Object read()
    throws FeaturestoreNotFound, JAXBException, TrainingDatasetFormatNotSupportedError,
    TrainingDatasetDoesNotExistError, IOException, FeaturestoresNotFound, JWTNotFoundException, HiveNotEnabled;
  
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
   */
  public abstract void write()
    throws FeaturegroupDeletionError, DataframeIsEmpty, SparkDataTypeNotRecognizedError,
    JAXBException, FeaturegroupUpdateStatsError, FeaturestoreNotFound, TrainingDatasetDoesNotExistError,
    TrainingDatasetFormatNotSupportedError, IOException, InvalidPrimaryKeyForFeaturegroup, FeaturegroupCreationError,
    TrainingDatasetCreationError, CannotWriteImageDataFrameException, JWTNotFoundException,
    FeaturegroupDoesNotExistError, HiveNotEnabled;
}
