package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.Constants;
import io.hops.util.FeaturestoreRestClient;
import io.hops.util.Hops;
import io.hops.util.exceptions.CannotWriteImageDataFrameException;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.SparkDataTypeNotRecognizedError;
import io.hops.util.exceptions.StorageConnectorDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetCreationError;
import io.hops.util.exceptions.TrainingDatasetFormatNotSupportedError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.feature.FeatureDTO;
import io.hops.util.featurestore.dtos.stats.StatisticsDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreS3ConnectorDTO;
import io.hops.util.featurestore.dtos.trainingdataset.ExternalTrainingDatasetDTO;
import io.hops.util.featurestore.dtos.trainingdataset.HopsfsTrainingDatasetDTO;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetDTO;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetType;
import io.hops.util.featurestore.ops.FeaturestoreOp;
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
   * @throws HiveNotEnabled HiveNotEnabled
   */
  public void write()
      throws DataframeIsEmpty, SparkDataTypeNotRecognizedError,
      JAXBException, FeaturestoreNotFound,
      TrainingDatasetCreationError, TrainingDatasetFormatNotSupportedError, CannotWriteImageDataFrameException,
      JWTNotFoundException, HiveNotEnabled, StorageConnectorDoesNotExistError {
    if(dataframe == null) {
      throw new IllegalArgumentException("Dataframe to create featuregroup from cannot be null, specify dataframe " +
        "with " +
        ".setDataframe(df)");
    }
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(name, getSpark(),
      dataframe, featurestore, version, descriptiveStats, featureCorr, featureHistograms,
      clusterAnalysis, statColumns, numBins, numClusters, corrMethod);
    List<FeatureDTO> featuresSchema = FeaturestoreHelper.parseSparkFeaturesSchema(dataframe.schema(), null,
      null);
    FeaturestoreMetadataDTO featurestoreMetadata = FeaturestoreHelper.getFeaturestoreMetadataCache();
    if(hopsfs){
      doCreateHopsfsTrainingDataset(featurestoreMetadata, statisticsDTO, featuresSchema);
    } else {
      doCreateExternalTrainingDataset(featurestoreMetadata, statisticsDTO, featuresSchema);
    }
    //Update metadata cache since we created a new training dataset
    Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
  }
  
  /**
   * Groups input parameters into a DTO representation for Hopsfs Training Datasets
   *
   * @param statisticsDTO statistics computed based on the dataframe
   * @param features features of the training dataset (inferred from the dataframe)
   * @param hopsfsStorageConnectorId id of the hopsfs storage connector linked to the training dataset
   * @return HopsfsTrainingDatasetDTO
   */
  private HopsfsTrainingDatasetDTO groupInputParamsIntoHopsfsDTO(StatisticsDTO statisticsDTO,
    List<FeatureDTO> features, Integer hopsfsStorageConnectorId){
    HopsfsTrainingDatasetDTO hopsfsTrainingDatasetDTO = new HopsfsTrainingDatasetDTO();
    hopsfsTrainingDatasetDTO.setFeaturestoreName(featurestore);
    hopsfsTrainingDatasetDTO.setName(name);
    hopsfsTrainingDatasetDTO.setVersion(version);
    hopsfsTrainingDatasetDTO.setDescription(description);
    hopsfsTrainingDatasetDTO.setJobName(jobName);
    hopsfsTrainingDatasetDTO.setDataFormat(dataFormat);
    hopsfsTrainingDatasetDTO.setFeatures(features);
    hopsfsTrainingDatasetDTO.setDescriptiveStatistics(statisticsDTO.getDescriptiveStatsDTO());
    hopsfsTrainingDatasetDTO.setFeatureCorrelationMatrix(statisticsDTO.getFeatureCorrelationMatrixDTO());
    hopsfsTrainingDatasetDTO.setFeaturesHistogram(statisticsDTO.getFeatureDistributionsDTO());
    hopsfsTrainingDatasetDTO.setClusterAnalysis(statisticsDTO.getClusterAnalysisDTO());
    hopsfsTrainingDatasetDTO.setHopsfsConnectorId(hopsfsStorageConnectorId);
    hopsfsTrainingDatasetDTO.setTrainingDatasetType(TrainingDatasetType.HOPSFS_TRAINING_DATASET);
    return hopsfsTrainingDatasetDTO;
  }
  
  /**
   * Groups input parameters into a DTO representation for external Training Datasets
   *
   * @param statisticsDTO statistics computed based on the dataframe
   * @param features features of the training dataset (inferred from the dataframe)
   * @param s3ConnectorId id of the s3 connector linked to the training dataset
   * @return ExternalTrainingDatasetDTO
   */
  private ExternalTrainingDatasetDTO groupInputParamsIntoExternalDTO(StatisticsDTO statisticsDTO,
    List<FeatureDTO> features, Integer s3ConnectorId){
    ExternalTrainingDatasetDTO externalTrainingDatasetDTO = new ExternalTrainingDatasetDTO();
    externalTrainingDatasetDTO.setFeaturestoreName(featurestore);
    externalTrainingDatasetDTO.setName(name);
    externalTrainingDatasetDTO.setVersion(version);
    externalTrainingDatasetDTO.setDescription(description);
    externalTrainingDatasetDTO.setJobName(jobName);
    externalTrainingDatasetDTO.setDataFormat(dataFormat);
    externalTrainingDatasetDTO.setFeatures(features);
    externalTrainingDatasetDTO.setDescriptiveStatistics(statisticsDTO.getDescriptiveStatsDTO());
    externalTrainingDatasetDTO.setFeatureCorrelationMatrix(statisticsDTO.getFeatureCorrelationMatrixDTO());
    externalTrainingDatasetDTO.setFeaturesHistogram(statisticsDTO.getFeatureDistributionsDTO());
    externalTrainingDatasetDTO.setClusterAnalysis(statisticsDTO.getClusterAnalysisDTO());
    externalTrainingDatasetDTO.setS3ConnectorId(s3ConnectorId);
    externalTrainingDatasetDTO.setTrainingDatasetType(TrainingDatasetType.EXTERNAL_TRAINING_DATASET);
    return externalTrainingDatasetDTO;
  }
  
  /**
   * Creates a new HopsFS training dataset (synchronizes it with Hopsworks using the REST API and writes it to HopsFS
   * using Spark)
   *
   * @param featurestoreMetadataDTO metadata about the feature store
   * @param statisticsDTO statistics about the training dataset
   * @param featuresSchema
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws TrainingDatasetCreationError TrainingDatasetCreationError
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws CannotWriteImageDataFrameException CannotWriteImageDataFrameException
   */
  private void doCreateHopsfsTrainingDataset(FeaturestoreMetadataDTO featurestoreMetadataDTO,
    StatisticsDTO statisticsDTO, List<FeatureDTO> featuresSchema)
    throws StorageConnectorDoesNotExistError, JAXBException, FeaturestoreNotFound, TrainingDatasetCreationError,
    JWTNotFoundException, HiveNotEnabled, TrainingDatasetFormatNotSupportedError, CannotWriteImageDataFrameException {
    Integer storageConnectorId;
    if(storageConnector != null){
      storageConnectorId = FeaturestoreHelper.findStorageConnector(featurestoreMetadataDTO.getStorageConnectors(),
        storageConnector).getId();
    } else {
      storageConnectorId = FeaturestoreHelper.findStorageConnector(featurestoreMetadataDTO.getStorageConnectors(),
        FeaturestoreHelper.getProjectTrainingDatasetsSink()).getId();
    }
    Response response = FeaturestoreRestClient.createTrainingDatasetRest(groupInputParamsIntoHopsfsDTO(statisticsDTO,
      featuresSchema, storageConnectorId), featurestoreMetadataDTO.getSettings().getHopsfsTrainingDatasetDtoType());
    String jsonStrResponse = response.readEntity(String.class);
    JSONObject jsonObjResponse = new JSONObject(jsonStrResponse);
    TrainingDatasetDTO trainingDatasetDTO = FeaturestoreHelper.parseTrainingDatasetJson(jsonObjResponse);
    HopsfsTrainingDatasetDTO hopsfsTrainingDatasetDTO = (HopsfsTrainingDatasetDTO) trainingDatasetDTO;
    String hdfsPath = hopsfsTrainingDatasetDTO.getHdfsStorePath() + Constants.SLASH_DELIMITER + name;
    FeaturestoreHelper.writeTrainingDatasetHdfs(getSpark(), dataframe, hdfsPath, dataFormat,
      Constants.SPARK_OVERWRITE_MODE);
    if (dataFormat.equals(Constants.TRAINING_DATASET_TFRECORDS_FORMAT)) {
      try {
        JSONObject tfRecordSchemaJson = FeaturestoreHelper.getDataframeTfRecordSchemaJson(dataframe);
        FeaturestoreHelper.writeTfRecordSchemaJson(hopsfsTrainingDatasetDTO.getHdfsStorePath()
            + Constants.SLASH_DELIMITER + Constants.TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME,
          tfRecordSchemaJson.toString());
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Could not save tf record schema json to HDFS for training dataset: " + name, e);
      }
    }
  }
  
  /**
   * Creates a new external training dataset (synchronizes it with Hopsworks using the REST API and writes it to the
   * external store using Spark (e.g to S3))
   *
   * @param featurestoreMetadataDTO metadata about the featurestore
   * @param statisticsDTO statistics about the training dataset
   * @param featuresSchema schema of the training dataset
   * @throws StorageConnectorDoesNotExistError
   * @throws FeaturestoreNotFound
   * @throws JWTNotFoundException
   * @throws TrainingDatasetCreationError
   * @throws JAXBException
   * @throws HiveNotEnabled
   */
  private void doCreateExternalTrainingDataset(FeaturestoreMetadataDTO featurestoreMetadataDTO,
    StatisticsDTO statisticsDTO, List<FeatureDTO> featuresSchema)
    throws StorageConnectorDoesNotExistError, FeaturestoreNotFound, JWTNotFoundException, TrainingDatasetCreationError,
    JAXBException, HiveNotEnabled {
    FeaturestoreS3ConnectorDTO s3ConnectorDTO;
    if(storageConnector != null){
      s3ConnectorDTO = (FeaturestoreS3ConnectorDTO) FeaturestoreHelper.findStorageConnector(
        featurestoreMetadataDTO.getStorageConnectors(),
        storageConnector);
    } else {
      throw new IllegalArgumentException("Storage Connector cannot be null for External Training Datasets");
    }
    String path = s3ConnectorDTO.getBucket() + Constants.SLASH_DELIMITER + FeaturestoreHelper.getTableName(
      name, version);
    FeaturestoreRestClient.createTrainingDatasetRest(groupInputParamsIntoExternalDTO(statisticsDTO, featuresSchema,
        s3ConnectorDTO.getId()), featurestoreMetadataDTO.getSettings().getExternalTrainingDatasetDtoType());
    FeaturestoreHelper.writeTrainingDatasetS3(getSpark(), dataframe, path, dataFormat,
      Constants.SPARK_OVERWRITE_MODE);
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
  
  public FeaturestoreCreateTrainingDataset setDataFormat(String dataFormat) {
    this.dataFormat = dataFormat;
    return this;
  }

  public FeaturestoreCreateTrainingDataset setHopsfs(Boolean hopsfs) {
    this.hopsfs = hopsfs;
    return this;
  }

  public FeaturestoreCreateTrainingDataset setExternal(Boolean external) {
    this.external = external;
    return this;
  }

  public FeaturestoreCreateTrainingDataset setStorageConnector(String storageConnector) {
    this.storageConnector = storageConnector;
    return this;
  }
  
}
