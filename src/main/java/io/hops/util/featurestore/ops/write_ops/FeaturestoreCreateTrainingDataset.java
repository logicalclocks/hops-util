package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.Constants;
import io.hops.util.FeaturestoreRestClient;
import io.hops.util.Hops;
import io.hops.util.exceptions.CannotWriteImageDataFrameException;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.OnlineFeaturestoreNotEnabled;
import io.hops.util.exceptions.OnlineFeaturestorePasswordNotFound;
import io.hops.util.exceptions.OnlineFeaturestoreUserNotFound;
import io.hops.util.exceptions.SparkDataTypeNotRecognizedError;
import io.hops.util.exceptions.StorageConnectorDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetCreationError;
import io.hops.util.exceptions.TrainingDatasetFormatNotSupportedError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.feature.FeatureDTO;
import io.hops.util.featurestore.dtos.jobs.FeaturestoreJobDTO;
import io.hops.util.featurestore.dtos.stats.StatisticsDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreHopsfsConnectorDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreS3ConnectorDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreStorageConnectorDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreStorageConnectorType;
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
import java.util.stream.Collectors;

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
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  public void write()
    throws DataframeIsEmpty, SparkDataTypeNotRecognizedError,
    JAXBException, FeaturestoreNotFound,
    TrainingDatasetCreationError, TrainingDatasetFormatNotSupportedError, CannotWriteImageDataFrameException,
    JWTNotFoundException, HiveNotEnabled, StorageConnectorDoesNotExistError, OnlineFeaturestoreUserNotFound,
    OnlineFeaturestorePasswordNotFound, OnlineFeaturestoreNotEnabled {
    if(dataframe == null) {
      throw new IllegalArgumentException("Dataframe to create featuregroup from cannot be null, specify dataframe " +
        "with .setDataframe(df)");
    }
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(name, getSpark(),
      dataframe, featurestore, version, descriptiveStats, featureCorr, featureHistograms,
      clusterAnalysis, statColumns, numBins, numClusters, corrMethod);
    List<FeatureDTO> featuresSchema = FeaturestoreHelper.parseSparkFeaturesSchema(dataframe.schema(), null,
      null, false, null);
    FeaturestoreMetadataDTO featurestoreMetadata = FeaturestoreHelper.getFeaturestoreMetadataCache();
    FeaturestoreStorageConnectorDTO storageConnectorDTO;
    if(sink != null){
      storageConnectorDTO =
        FeaturestoreHelper.findStorageConnector(featurestoreMetadata.getStorageConnectors(),
          sink);
    } else {
      storageConnectorDTO = FeaturestoreHelper.findStorageConnector(featurestoreMetadata.getStorageConnectors(),
        FeaturestoreHelper.getProjectTrainingDatasetsSink());
    }
    if(storageConnectorDTO.getStorageConnectorType() == FeaturestoreStorageConnectorType.S3) {
      doCreateExternalTrainingDataset(featurestoreMetadata, statisticsDTO, featuresSchema,
        (FeaturestoreS3ConnectorDTO) storageConnectorDTO);
    } else {
      doCreateHopsfsTrainingDataset(featurestoreMetadata, statisticsDTO, featuresSchema,
        (FeaturestoreHopsfsConnectorDTO) storageConnectorDTO);
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
    List<FeatureDTO> features, Integer hopsfsStorageConnectorId) {
    if(FeaturestoreHelper.jobNameGetOrDefault(null) != null){
      jobs.add(FeaturestoreHelper.jobNameGetOrDefault(null));
    }
    List<FeaturestoreJobDTO> jobsDTOs = jobs.stream().map(jobName -> {
      FeaturestoreJobDTO featurestoreJobDTO = new FeaturestoreJobDTO();
      featurestoreJobDTO.setJobName(jobName);
      return featurestoreJobDTO;
    }).collect(Collectors.toList());
    HopsfsTrainingDatasetDTO hopsfsTrainingDatasetDTO = new HopsfsTrainingDatasetDTO();
    hopsfsTrainingDatasetDTO.setFeaturestoreName(featurestore);
    hopsfsTrainingDatasetDTO.setName(name);
    hopsfsTrainingDatasetDTO.setVersion(version);
    hopsfsTrainingDatasetDTO.setDescription(description);
    hopsfsTrainingDatasetDTO.setJobs(jobsDTOs);
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
    if(FeaturestoreHelper.jobNameGetOrDefault(null) != null){
      jobs.add(FeaturestoreHelper.jobNameGetOrDefault(null));
    }
    List<FeaturestoreJobDTO> jobsDTOs = jobs.stream().map(jobName -> {
      FeaturestoreJobDTO featurestoreJobDTO = new FeaturestoreJobDTO();
      featurestoreJobDTO.setJobName(jobName);
      return featurestoreJobDTO;
    }).collect(Collectors.toList());
    ExternalTrainingDatasetDTO externalTrainingDatasetDTO = new ExternalTrainingDatasetDTO();
    externalTrainingDatasetDTO.setFeaturestoreName(featurestore);
    externalTrainingDatasetDTO.setName(name);
    externalTrainingDatasetDTO.setVersion(version);
    externalTrainingDatasetDTO.setDescription(description);
    externalTrainingDatasetDTO.setJobs(jobsDTOs);
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
   * @param featuresSchema schema of the training dataset
   * @param featurestoreHopsfsConnectorDTO the HopsFS storage connector
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
    StatisticsDTO statisticsDTO, List<FeatureDTO> featuresSchema,
    FeaturestoreHopsfsConnectorDTO featurestoreHopsfsConnectorDTO)
    throws JAXBException, FeaturestoreNotFound, TrainingDatasetCreationError,
    JWTNotFoundException, HiveNotEnabled, TrainingDatasetFormatNotSupportedError, CannotWriteImageDataFrameException {
    Response response = FeaturestoreRestClient.createTrainingDatasetRest(groupInputParamsIntoHopsfsDTO(statisticsDTO,
      featuresSchema, featurestoreHopsfsConnectorDTO.getId()),
      featurestoreMetadataDTO.getSettings().getHopsfsTrainingDatasetDtoType());
    String jsonStrResponse = response.readEntity(String.class);
    JSONObject jsonObjResponse = new JSONObject(jsonStrResponse);
    TrainingDatasetDTO trainingDatasetDTO = FeaturestoreHelper.parseTrainingDatasetJson(jsonObjResponse);
    HopsfsTrainingDatasetDTO hopsfsTrainingDatasetDTO = (HopsfsTrainingDatasetDTO) trainingDatasetDTO;
    String hdfsPath = hopsfsTrainingDatasetDTO.getHdfsStorePath() + Constants.SLASH_DELIMITER + name;
    FeaturestoreHelper.writeTrainingDataset(getSpark(), dataframe, hdfsPath, dataFormat,
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
   * @param s3ConnectorDTO S3 connector
   * @throws FeaturestoreNotFound
   * @throws JWTNotFoundException
   * @throws TrainingDatasetCreationError
   * @throws JAXBException
   * @throws HiveNotEnabled
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws CannotWriteImageDataFrameException CannotWriteImageDataFrameException
   */
  private void doCreateExternalTrainingDataset(FeaturestoreMetadataDTO featurestoreMetadataDTO,
    StatisticsDTO statisticsDTO, List<FeatureDTO> featuresSchema,
    FeaturestoreS3ConnectorDTO s3ConnectorDTO)
    throws FeaturestoreNotFound, JWTNotFoundException, TrainingDatasetCreationError,
    JAXBException, HiveNotEnabled, TrainingDatasetFormatNotSupportedError, CannotWriteImageDataFrameException {
    String path = FeaturestoreHelper.getExternalTrainingDatasetPath(name, version, s3ConnectorDTO.getBucket());
    FeaturestoreHelper.setupS3CredentialsForSpark(s3ConnectorDTO.getAccessKey(), s3ConnectorDTO.getSecretKey(),
      getSpark());
    FeaturestoreRestClient.createTrainingDatasetRest(groupInputParamsIntoExternalDTO(statisticsDTO, featuresSchema,
        s3ConnectorDTO.getId()), featurestoreMetadataDTO.getSettings().getExternalTrainingDatasetDtoType());
    FeaturestoreHelper.writeTrainingDataset(getSpark(), dataframe, path, dataFormat,
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
  
  public FeaturestoreCreateTrainingDataset setJobs(List<String> jobs) {
    this.jobs = jobs;
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
  
  public FeaturestoreCreateTrainingDataset setSink(String sink) {
    this.sink = sink;
    return this;
  }

  
  
}
