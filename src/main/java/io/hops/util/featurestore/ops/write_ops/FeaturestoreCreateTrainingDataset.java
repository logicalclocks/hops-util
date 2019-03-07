package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.Constants;
import io.hops.util.FeaturestoreRestClient;
import io.hops.util.Hops;
import io.hops.util.exceptions.CannotWriteImageDataFrameException;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.SparkDataTypeNotRecognizedError;
import io.hops.util.exceptions.TrainingDatasetCreationError;
import io.hops.util.exceptions.TrainingDatasetFormatNotSupportedError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.feature.FeatureDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import io.hops.util.featurestore.stats.StatisticsDTO;
import io.hops.util.featurestore.trainingdataset.TrainingDatasetDTO;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;

import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Builder class for Create-TrainingDataset operation on the Hopsworks Featurestore
 */
public class FeaturestoreCreateTrainingDataset extends FeaturestoreOp {
  
  private static final Logger LOG = Logger.getLogger(FeaturestoreCreateTrainingDataset.class.getName());
  
  /**
   * Constructor
   *
   * @param name name of the training dataset to create
   */
  public FeaturestoreCreateTrainingDataset(String name) {
    super(name);
  }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }
  

  /**
   * Creates a new training dataset in the featurestore
   *
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws TrainingDatasetCreationError TrainingDatasetCreationError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws CannotWriteImageDataFrameException CannotWriteImageDataFrameException
   */
  public void write()
    throws DataframeIsEmpty, SparkDataTypeNotRecognizedError,
    JAXBException, FeaturestoreNotFound,
    TrainingDatasetCreationError, TrainingDatasetFormatNotSupportedError, CannotWriteImageDataFrameException,
    JWTNotFoundException {
    if(dataframe == null) {
      throw new IllegalArgumentException("Dataframe to create featuregroup from cannot be null, specify dataframe " +
        "with " +
        ".setDataframe(df)");
    }
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(name, spark,
      dataframe, featurestore, version, descriptiveStats, featureCorr, featureHistograms,
      clusterAnalysis, statColumns, numBins, numClusters, corrMethod);
    List<FeatureDTO> featuresSchema = FeaturestoreHelper.parseSparkFeaturesSchema(dataframe.schema(), null);
    Response response = FeaturestoreRestClient.createTrainingDatasetRest(featurestore, name, version, description,
      jobName, dataFormat, dependencies, featuresSchema, statisticsDTO);
    String jsonStrResponse = response.readEntity(String.class);
    JSONObject jsonObjResponse = new JSONObject(jsonStrResponse);
    TrainingDatasetDTO trainingDatasetDTO = FeaturestoreHelper.parseTrainingDatasetJson(jsonObjResponse);
    String hdfsPath = trainingDatasetDTO.getHdfsStorePath() + Constants.SLASH_DELIMITER + name;
    FeaturestoreHelper.writeTrainingDatasetHdfs(spark, dataframe, hdfsPath, dataFormat, Constants.SPARK_OVERWRITE_MODE);
    if (dataFormat == Constants.TRAINING_DATASET_TFRECORDS_FORMAT) {
      try {
        JSONObject tfRecordSchemaJson = FeaturestoreHelper.getDataframeTfRecordSchemaJson(dataframe);
        FeaturestoreHelper.writeTfRecordSchemaJson(trainingDatasetDTO.getHdfsStorePath()
            + Constants.SLASH_DELIMITER + Constants.TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME,
          tfRecordSchemaJson.toString());
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Could not save tf record schema json to HDFS for training dataset: " + name, e);
      }
    }
    //Update metadata cache since we created a new training dataset
    Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
  }
  
  public FeaturestoreCreateTrainingDataset setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setVersion(int version) {
    this.version = version;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setCorrMethod(String corrMethod) {
    this.corrMethod = corrMethod;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setNumBins(int numBins) {
    this.numBins = numBins;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setNumClusters(int numClusters) {
    this.numClusters = numClusters;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setMode(String mode) {
    this.mode = mode;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setDataframe(Dataset<Row> dataframe) {
    this.dataframe = dataframe;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setDescriptiveStats(Boolean descriptiveStats) {
    this.descriptiveStats = descriptiveStats;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setFeatureCorr(Boolean featureCorr) {
    this.featureCorr = featureCorr;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setFeatureHistograms(Boolean featureHistograms) {
    this.featureHistograms = featureHistograms;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setClusterAnalysis(Boolean clusterAnalysis) {
    this.clusterAnalysis = clusterAnalysis;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setStatColumns(List<String> statColumns) {
    this.statColumns = statColumns;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setJobName(String jobName) {
    this.jobName = jobName;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setPrimaryKey(String primaryKey) {
    this.primaryKey = primaryKey;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setDescription(String description) {
    this.description = description;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setDependencies(List<String> dependencies) {
    this.dependencies = dependencies;
    return this;
  }
  
  public FeaturestoreCreateTrainingDataset setDataFormat(String dataFormat) {
    this.dataFormat = dataFormat;
    return this;
  }
  
}
