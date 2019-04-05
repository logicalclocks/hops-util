package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.FeaturestoreRestClient;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturegroupDeletionError;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.FeaturegroupUpdateStatsError;
import io.hops.util.exceptions.FeaturestoreNotFound;
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
 * Builder class for InsertInto-Featuregroup operation on the Hopsworks Featurestore
 */
public class FeaturestoreInsertIntoFeaturegroup extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the featuregroup to insert into
   */
  public FeaturestoreInsertIntoFeaturegroup(String name) {
    super(name);
  }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }
  
  /**
   * Inserts a dataframes with rows into a featuregroup
   *
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws FeaturegroupDeletionError FeaturegroupDeletionError
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   */
  public void write()
    throws DataframeIsEmpty, SparkDataTypeNotRecognizedError,
    JAXBException, FeaturegroupUpdateStatsError, FeaturestoreNotFound, JWTNotFoundException,
    FeaturegroupDeletionError, FeaturegroupDoesNotExistError {
    if(dataframe == null){
      throw new IllegalArgumentException("Dataframe to insert cannot be null, specify dataframe with " +
        ".setDataframe(df)");
    }
    spark.sparkContext().setJobGroup(
      "Inserting dataframe into featuregroup",
      "Inserting into featuregroup:" + name + " in the featurestore:" +
        featurestore, true);
    if (mode==null || !mode.equalsIgnoreCase("append") && !mode.equalsIgnoreCase("overwrite"))
      throw new IllegalArgumentException("The supplied write mode: " + mode +
        " does not match any of the supported modes: overwrite, append");
    if (mode.equalsIgnoreCase("overwrite")) {
      FeaturestoreRestClient.deleteTableContentsRest(featurestore, name, version);
      //update cache because in the background, clearing featuregroup will give it a new id
      new FeaturestoreUpdateMetadataCache().setFeaturestore(featurestore).write();
    }
    spark.sparkContext().setJobGroup("", "", true);
    FeaturestoreHelper.insertIntoFeaturegroup(dataframe, spark, name,
      featurestore, version);
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(name, spark, dataframe,
      featurestore, version,
      descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns, numBins, numClusters,
      corrMethod);
    FeaturestoreRestClient.updateFeaturegroupStatsRest(name, featurestore, version, statisticsDTO);
  }
  
  public FeaturestoreInsertIntoFeaturegroup setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setVersion(int version) {
    this.version = version;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setCorrMethod(String corrMethod) {
    this.corrMethod = corrMethod;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setNumBins(int numBins) {
    this.numBins = numBins;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setNumClusters(int numClusters) {
    this.numClusters = numClusters;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setMode(String mode) {
    this.mode = mode;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setDataframe(Dataset<Row> dataframe) {
    this.dataframe = dataframe;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setDescriptiveStats(Boolean descriptiveStats) {
    this.descriptiveStats = descriptiveStats;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setFeatureCorr(Boolean featureCorr) {
    this.featureCorr = featureCorr;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setFeatureHistograms(Boolean featureHistograms) {
    this.featureHistograms = featureHistograms;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setClusterAnalysis(Boolean clusterAnalysis) {
    this.clusterAnalysis = clusterAnalysis;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setStatColumns(List<String> statColumns) {
    this.statColumns = statColumns;
    return this;
  }
  
}
