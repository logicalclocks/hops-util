package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.Constants;
import io.hops.util.FeaturestoreRestClient;
import io.hops.util.Hops;
import io.hops.util.exceptions.CannotWriteImageDataFrameException;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturegroupUpdateStatsError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.SparkDataTypeNotRecognizedError;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetFormatNotSupportedError;
import io.hops.util.featurestore.FeaturegroupsAndTrainingDatasetsDTO;
import io.hops.util.featurestore.FeaturestoreHelper;
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
 * Builder class for InsertInto-TrainingDataset operation on the Hopsworks Featurestore
 */
public class FeaturestoreInsertIntoTrainingDataset extends FeaturestoreOp {
  
  private static final Logger LOG = Logger.getLogger(FeaturestoreInsertIntoTrainingDataset.class.getName());
  
  /**
   * Constructor
   *
   * @param name name of the training dataset to insert into
   */
  public FeaturestoreInsertIntoTrainingDataset(String name) {
    super(name);
  }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }
  

  /**
   * Inserts a dataframes with rows into a training dataset
   *
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws CannotWriteImageDataFrameException CannotWriteImageDataFrameException
   */
  public void write()
    throws DataframeIsEmpty, SparkDataTypeNotRecognizedError,
    JAXBException, FeaturegroupUpdateStatsError, FeaturestoreNotFound, TrainingDatasetDoesNotExistError,
    TrainingDatasetFormatNotSupportedError, CannotWriteImageDataFrameException, JWTNotFoundException {
    if(dataframe == null){
      throw new IllegalArgumentException("Dataframe to insert cannot be null, specify dataframe with " +
        ".setDataframe(df)");
    }
    if (mode==null || !mode.equalsIgnoreCase("overwrite"))
      throw new IllegalArgumentException("The supplied write mode: " + mode +
        " does not match any of the supported modes: overwrite (training datasets are immutable)");
    try {
      doInsertIntoTrainingDataset(spark, dataframe, name, featurestore,
        Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read(), version,
        descriptiveStats, featureCorr,
        featureHistograms, clusterAnalysis, statColumns, numBins, corrMethod, numClusters,
        mode);
    } catch (Exception e) {
      Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
      doInsertIntoTrainingDataset(spark, dataframe, name, featurestore,
        Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read(), version,
        descriptiveStats, featureCorr,
        featureHistograms, clusterAnalysis, statColumns, numBins, corrMethod, numClusters,
        mode);
    }
  }
  
  /**
   * Inserts a spark dataframe into an existing training dataset (append/overwrite)
   *
   * @param sparkDf                the spark dataframe to insert
   * @param sparkSession           the spark session
   * @param trainingDataset        the name of the training dataset to insert into
   * @param featurestore           the name of the featurestore where the training dataset resides
   * @param featurestoreMetadata   metadata of the featurestore to query
   * @param trainingDatasetVersion the version of the training dataset
   * @param descriptiveStats       a boolean flag whether to compute descriptive statistics of the new data
   * @param featureCorr            a boolean flag whether to compute feature correlation analysis of the new data
   * @param featureHistograms      a boolean flag whether to compute feature histograms of the new data
   * @param clusterAnalysis        a boolean flag whether to compute cluster analysis of the new data
   * @param statColumns            a list of columns to compute statistics for (defaults to all columns
   *                               that are numeric)
   * @param numBins                number of bins to use for computing histograms
   * @param corrMethod             the method to compute feature correlation with (pearson or spearman)
   * @param numClusters            number of clusters to use for cluster analysis
   * @param writeMode              the spark write mode (append/overwrite)
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws JWTNotFoundException JWTNotFoundException
   */
  private static void doInsertIntoTrainingDataset(
    SparkSession sparkSession, Dataset<Row> sparkDf, String trainingDataset,
    String featurestore, FeaturegroupsAndTrainingDatasetsDTO featurestoreMetadata, int trainingDatasetVersion,
    Boolean descriptiveStats, Boolean featureCorr,
    Boolean featureHistograms, Boolean clusterAnalysis, List<String> statColumns, Integer numBins,
    String corrMethod, Integer numClusters, String writeMode)
    throws JAXBException, TrainingDatasetDoesNotExistError, DataframeIsEmpty, FeaturegroupUpdateStatsError,
    TrainingDatasetFormatNotSupportedError, SparkDataTypeNotRecognizedError, FeaturestoreNotFound,
    CannotWriteImageDataFrameException, JWTNotFoundException {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    sparkSession = FeaturestoreHelper.sparkGetOrDefault(sparkSession);
    corrMethod = FeaturestoreHelper.correlationMethodGetOrDefault(corrMethod);
    numBins = FeaturestoreHelper.numBinsGetOrDefault(numBins);
    numClusters = FeaturestoreHelper.numClustersGetOrDefault(numClusters);
    List<TrainingDatasetDTO> trainingDatasetDTOList = featurestoreMetadata.getTrainingDatasets();
    FeaturestoreHelper.findTrainingDataset(trainingDatasetDTOList,
      trainingDataset, trainingDatasetVersion);
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(trainingDataset, sparkSession,
      sparkDf,
      featurestore, trainingDatasetVersion,
      descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns, numBins, numClusters,
      corrMethod);
    Response response = FeaturestoreRestClient.updateTrainingDatasetStatsRest(trainingDataset, featurestore,
      trainingDatasetVersion,
      statisticsDTO);
    String jsonStrResponse = response.readEntity(String.class);
    JSONObject jsonObjResponse = new JSONObject(jsonStrResponse);
    TrainingDatasetDTO updatedTrainingDatasetDTO = FeaturestoreHelper.parseTrainingDatasetJson(jsonObjResponse);
    String hdfsPath = updatedTrainingDatasetDTO.getHdfsStorePath() + "/" + trainingDataset;
    FeaturestoreHelper.writeTrainingDatasetHdfs(sparkSession, sparkDf, hdfsPath,
      updatedTrainingDatasetDTO.getDataFormat(), writeMode);
    if (updatedTrainingDatasetDTO.getDataFormat() == Constants.TRAINING_DATASET_TFRECORDS_FORMAT) {
      JSONObject tfRecordSchemaJson = null;
      try{
        tfRecordSchemaJson = FeaturestoreHelper.getDataframeTfRecordSchemaJson(sparkDf);
      } catch (Exception e){
        LOG.log(Level.WARNING, "Could not infer the TF-record schema for the training dataset");
      }
      if(tfRecordSchemaJson != null){
        try {
          FeaturestoreHelper.writeTfRecordSchemaJson(updatedTrainingDatasetDTO.getHdfsStorePath()
              + Constants.SLASH_DELIMITER + Constants.TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME,
            tfRecordSchemaJson.toString());
        } catch (Exception e) {
          LOG.log(Level.WARNING, "Could not save tf record schema json to HDFS for training dataset: "
            + trainingDataset, e);
        }
      }
    }
  }
  
  public FeaturestoreInsertIntoTrainingDataset setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreInsertIntoTrainingDataset setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreInsertIntoTrainingDataset setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
  
  public FeaturestoreInsertIntoTrainingDataset setVersion(int version) {
    this.version = version;
    return this;
  }
  
  public FeaturestoreInsertIntoTrainingDataset setCorrMethod(String corrMethod) {
    this.corrMethod = corrMethod;
    return this;
  }
  
  public FeaturestoreInsertIntoTrainingDataset setNumBins(int numBins) {
    this.numBins = numBins;
    return this;
  }
  
  public FeaturestoreInsertIntoTrainingDataset setNumClusters(int numClusters) {
    this.numClusters = numClusters;
    return this;
  }
  
  public FeaturestoreInsertIntoTrainingDataset setMode(String mode) {
    this.mode = mode;
    return this;
  }
  
  public FeaturestoreInsertIntoTrainingDataset setDataframe(Dataset<Row> dataframe) {
    this.dataframe = dataframe;
    return this;
  }
  
  public FeaturestoreInsertIntoTrainingDataset setDescriptiveStats(Boolean descriptiveStats) {
    this.descriptiveStats = descriptiveStats;
    return this;
  }
  
  public FeaturestoreInsertIntoTrainingDataset setFeatureCorr(Boolean featureCorr) {
    this.featureCorr = featureCorr;
    return this;
  }
  
  public FeaturestoreInsertIntoTrainingDataset setFeatureHistograms(Boolean featureHistograms) {
    this.featureHistograms = featureHistograms;
    return this;
  }
  
  public FeaturestoreInsertIntoTrainingDataset setClusterAnalysis(Boolean clusterAnalysis) {
    this.clusterAnalysis = clusterAnalysis;
    return this;
  }
  
  public FeaturestoreInsertIntoTrainingDataset setStatColumns(List<String> statColumns) {
    this.statColumns = statColumns;
    return this;
  }
  
}
