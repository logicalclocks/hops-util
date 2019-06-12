package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.FeaturestoreRestClient;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.FeaturegroupUpdateStatsError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.SparkDataTypeNotRecognizedError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import io.hops.util.featurestore.dtos.stats.StatisticsDTO;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.bind.JAXBException;
import java.util.List;

/**
 * Builder class for Update-Featuregroup-Stats operation on the Hopsworks Featurestore
 */
public class FeaturestoreUpdateFeaturegroupStats extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the featuregroup to update stats of
   */
  public FeaturestoreUpdateFeaturegroupStats(String name) {
    super(name);
  }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }
  
  /**
   * Updates the stats of a featuregroup
   *
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws HiveNotEnabled HiveNotEnabled
   */
  public void write()
    throws DataframeIsEmpty, SparkDataTypeNotRecognizedError,
    JAXBException, FeaturegroupUpdateStatsError, FeaturestoreNotFound, JWTNotFoundException,
    FeaturegroupDoesNotExistError, HiveNotEnabled {
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(name, getSpark(), dataframe,
      featurestore, version, descriptiveStats, featureCorr, featureHistograms, clusterAnalysis,
      statColumns, numBins, numClusters, corrMethod);
    FeaturestoreRestClient.updateFeaturegroupStatsRest(name, featurestore, version, statisticsDTO);
  }
  
  public FeaturestoreUpdateFeaturegroupStats setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setVersion(int version) {
    this.version = version;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setCorrMethod(String corrMethod) {
    this.corrMethod = corrMethod;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setNumBins(int numBins) {
    this.numBins = numBins;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setNumClusters(int numClusters) {
    this.numClusters = numClusters;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setMode(String mode) {
    this.mode = mode;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setDataframe(Dataset<Row> dataframe) {
    this.dataframe = dataframe;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setDescriptiveStats(Boolean descriptiveStats) {
    this.descriptiveStats = descriptiveStats;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setFeatureCorr(Boolean featureCorr) {
    this.featureCorr = featureCorr;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setFeatureHistograms(Boolean featureHistograms) {
    this.featureHistograms = featureHistograms;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setClusterAnalysis(Boolean clusterAnalysis) {
    this.clusterAnalysis = clusterAnalysis;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setStatColumns(List<String> statColumns) {
    this.statColumns = statColumns;
    return this;
  }
  
}
