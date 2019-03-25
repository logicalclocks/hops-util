package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.FeaturestoreRestClient;
import io.hops.util.Hops;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturegroupCreationError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.InvalidPrimaryKeyForFeaturegroup;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.SparkDataTypeNotRecognizedError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.FeatureDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import io.hops.util.featurestore.dtos.stats.StatisticsDTO;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.bind.JAXBException;
import java.util.List;

/**
 * Builder class for Create-Featuregroup operation on the Hopsworks Featurestore
 */
public class FeaturestoreCreateFeaturegroup extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the featuregroup to create
   */
  public FeaturestoreCreateFeaturegroup(String name) {
    super(name);
  }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }

  /**
   * Creates a new feature group in the featurestore
   *
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws JAXBException JAXBException
   * @throws InvalidPrimaryKeyForFeaturegroup InvalidPrimaryKeyForFeaturegroup
   * @throws FeaturegroupCreationError FeaturegroupCreationError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JWTNotFoundException JWTNotFoundException
   */
  public void write()
    throws DataframeIsEmpty, SparkDataTypeNotRecognizedError,
    JAXBException, InvalidPrimaryKeyForFeaturegroup, FeaturegroupCreationError, FeaturestoreNotFound,
    JWTNotFoundException {
    if(dataframe == null) {
      throw new IllegalArgumentException("Dataframe to create featuregroup from cannot be null, specify dataframe " +
        "with " +
        ".setDataframe(df)");
    }
    primaryKey = FeaturestoreHelper.primaryKeyGetOrDefault(primaryKey, dataframe);
    FeaturestoreHelper.validatePrimaryKey(dataframe, primaryKey);
    FeaturestoreHelper.validateMetadata(name, dataframe.dtypes(), dependencies, description);
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(name, spark, dataframe,
      featurestore, version, descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns,
      numBins, numClusters, corrMethod);
    List<FeatureDTO> featuresSchema = FeaturestoreHelper.parseSparkFeaturesSchema(dataframe.schema(), primaryKey);
    FeaturestoreRestClient.createFeaturegroupRest(featurestore, name, version, description, jobName, dependencies,
      featuresSchema, statisticsDTO);
    FeaturestoreHelper.insertIntoFeaturegroup(dataframe, spark, name,
      featurestore, version);
    //Update metadata cache since we created a new feature group
    Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
    
  }
  
  public FeaturestoreCreateFeaturegroup setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setVersion(int version) {
    this.version = version;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setCorrMethod(String corrMethod) {
    this.corrMethod = corrMethod;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setNumBins(int numBins) {
    this.numBins = numBins;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setNumClusters(int numClusters) {
    this.numClusters = numClusters;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setMode(String mode) {
    this.mode = mode;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setDataframe(Dataset<Row> dataframe) {
    this.dataframe = dataframe;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setDescriptiveStats(Boolean descriptiveStats) {
    this.descriptiveStats = descriptiveStats;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setFeatureCorr(Boolean featureCorr) {
    this.featureCorr = featureCorr;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setFeatureHistograms(Boolean featureHistograms) {
    this.featureHistograms = featureHistograms;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setClusterAnalysis(Boolean clusterAnalysis) {
    this.clusterAnalysis = clusterAnalysis;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setStatColumns(List<String> statColumns) {
    this.statColumns = statColumns;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setJobName(String jobName) {
    this.jobName = jobName;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setPrimaryKey(String primaryKey) {
    this.primaryKey = primaryKey;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setDescription(String description) {
    this.description = description;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setDependencies(List<String> dependencies) {
    this.dependencies = dependencies;
    return this;
  }
  
}
