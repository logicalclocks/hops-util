package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.Constants;
import io.hops.util.Hops;
import io.hops.util.exceptions.CannotWriteImageDataFrameException;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.FeaturegroupUpdateStatsError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.OnlineFeaturestoreNotEnabled;
import io.hops.util.exceptions.OnlineFeaturestorePasswordNotFound;
import io.hops.util.exceptions.OnlineFeaturestoreUserNotFound;
import io.hops.util.exceptions.SparkDataTypeNotRecognizedError;
import io.hops.util.exceptions.StorageConnectorDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetFormatNotSupportedError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.jobs.FeaturestoreJobDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreS3ConnectorDTO;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetDTO;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetType;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;

import javax.xml.bind.JAXBException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   */
  public void write()
    throws DataframeIsEmpty, SparkDataTypeNotRecognizedError,
    JAXBException, FeaturegroupUpdateStatsError, FeaturestoreNotFound, TrainingDatasetDoesNotExistError,
    TrainingDatasetFormatNotSupportedError, CannotWriteImageDataFrameException, JWTNotFoundException, HiveNotEnabled,
    StorageConnectorDoesNotExistError, OnlineFeaturestoreUserNotFound, OnlineFeaturestorePasswordNotFound,
    OnlineFeaturestoreNotEnabled, FeaturegroupDoesNotExistError {
    if(dataframe == null){
      throw new IllegalArgumentException("Dataframe to insert cannot be null, specify dataframe with " +
        ".setDataframe(df)");
    }

    List<String> supportedModes = Arrays.asList(Constants.SPARK_OVERWRITE_MODE, Constants.SPARK_APPEND_MODE);
    if (mode==null || !supportedModes.stream().anyMatch(x -> x.equalsIgnoreCase(mode)))
      throw new IllegalArgumentException("The supplied write mode: " + mode +
        " does not match any of the supported modes: overwrite or append");
    try {
      doInsertIntoTrainingDataset(getSpark(), dataframe, name, featurestore,
        Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read(), version, mode);
    } catch (Exception e) {
      Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
      doInsertIntoTrainingDataset(getSpark(), dataframe, name, featurestore,
        Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read(), version, mode);
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
   * @param writeMode              the spark write mode (append/overwrite)
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   */
  private void doInsertIntoTrainingDataset(
      SparkSession sparkSession, Dataset<Row> sparkDf, String trainingDataset,
      String featurestore, FeaturestoreMetadataDTO featurestoreMetadata, int trainingDatasetVersion, String writeMode)
    throws JAXBException, TrainingDatasetDoesNotExistError, DataframeIsEmpty, FeaturegroupUpdateStatsError,
    TrainingDatasetFormatNotSupportedError, FeaturestoreNotFound,
    CannotWriteImageDataFrameException, JWTNotFoundException, StorageConnectorDoesNotExistError, HiveNotEnabled,
    OnlineFeaturestoreUserNotFound, OnlineFeaturestorePasswordNotFound, OnlineFeaturestoreNotEnabled,
    FeaturegroupDoesNotExistError {
    sparkSession = FeaturestoreHelper.sparkGetOrDefault(sparkSession);
    List<TrainingDatasetDTO> trainingDatasetDTOList = featurestoreMetadata.getTrainingDatasets();
    TrainingDatasetDTO trainingDatasetDTO = FeaturestoreHelper.findTrainingDataset(trainingDatasetDTOList,
      trainingDataset, trainingDatasetVersion);
    if(trainingDatasetDTO.getTrainingDatasetType() == TrainingDatasetType.HOPSFS_TRAINING_DATASET){
      insertIntoHopsfsTrainingDataset(trainingDatasetDTO, sparkSession, sparkDf, writeMode);
    } else {
      insertIntoExternalTrainingDataset(trainingDatasetDTO, featurestoreMetadata, sparkSession, sparkDf,
        writeMode);
    }
  }
  
  /**
   * Inserts into a Hopsfs training dataset using Spark
   *
   * @param trainingDatasetDTO DTO of the training dataset to insert into
   * @param sparkSession sparksesison to use for the insertion
   * @param sparkDf spark dataframe with data to insert in the training dataset
   * @param writeMode write mode for the operation (e.g append or overwrite)
   * @throws TrainingDatasetFormatNotSupportedError
   * @throws CannotWriteImageDataFrameException
   */
  private void insertIntoHopsfsTrainingDataset(TrainingDatasetDTO trainingDatasetDTO,
    SparkSession sparkSession, Dataset<Row> sparkDf, String writeMode)
    throws TrainingDatasetFormatNotSupportedError, CannotWriteImageDataFrameException {
    String hdfsPath = FeaturestoreHelper.getHopsfsTrainingDatasetPath(trainingDatasetDTO);
    FeaturestoreHelper.writeTrainingDataset(sparkSession, sparkDf, hdfsPath,
      trainingDatasetDTO.getDataFormat(), writeMode);
    if (trainingDatasetDTO.getDataFormat().equals(Constants.TRAINING_DATASET_TFRECORDS_FORMAT) ||
            trainingDatasetDTO.getDataFormat().equals(Constants.TRAINING_DATASET_TFRECORD_FORMAT)) {
      JSONObject tfRecordSchemaJson = null;
      try{
        tfRecordSchemaJson = FeaturestoreHelper.getDataframeTfRecordSchemaJson(sparkDf);
      } catch (Exception e){
        LOG.log(Level.WARNING, "Could not infer the TF-record schema for the training dataset");
      }
      if(tfRecordSchemaJson != null){
        try {
          FeaturestoreHelper.writeTfRecordSchemaJson(trainingDatasetDTO.getLocation() +
                  Constants.SLASH_DELIMITER + Constants.TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME,
              tfRecordSchemaJson.toString());
        } catch (Exception e) {
          LOG.log(Level.WARNING, "Could not save tf record schema json to HDFS for training dataset: "
            + trainingDatasetDTO.getName(), e);
        }
      }
    }
  }
  
  /**
   * Inserts into an external training dataset using Spark
   *
   * @param trainingDatasetDTO DTO of the training dataset
   * @param featurestoreMetadata metadata of the featurestore
   * @param sparkSession sparkSession to use for insertion
   * @param sparkDf dataframe with data to insert in the training dataset
   * @param writeMode write mode, e.g append or overwrite
   * @throws StorageConnectorDoesNotExistError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws CannotWriteImageDataFrameException CannotWriteImageDataFrameException
   * @throws HiveNotEnabled HiveNotEnabled
   */
  private void insertIntoExternalTrainingDataset(TrainingDatasetDTO trainingDatasetDTO,
    FeaturestoreMetadataDTO featurestoreMetadata, SparkSession sparkSession, Dataset<Row> sparkDf, String writeMode)
    throws StorageConnectorDoesNotExistError, TrainingDatasetFormatNotSupportedError,
    CannotWriteImageDataFrameException, HiveNotEnabled {
    FeaturestoreS3ConnectorDTO s3ConnectorDTO = (FeaturestoreS3ConnectorDTO) FeaturestoreHelper.findStorageConnector(
      featurestoreMetadata.getStorageConnectors(), trainingDatasetDTO.getStorageConnectorName());
    String path = FeaturestoreHelper.getExternalTrainingDatasetPath(trainingDatasetDTO);
    FeaturestoreHelper.setupS3CredentialsForSpark(s3ConnectorDTO.getAccessKey(), s3ConnectorDTO.getSecretKey(),
      getSpark());
    FeaturestoreHelper.writeTrainingDataset(sparkSession, sparkDf, path,
      trainingDatasetDTO.getDataFormat(), writeMode);
  }
  
  /**
   * Groups input parameters into a DTO
   *
   * @param trainingDatasetDTO DTO representation of the training dataset before updating the statistics
   * @return training dataset DTO
   */
  private TrainingDatasetDTO groupInputParamsIntoDTO(TrainingDatasetDTO trainingDatasetDTO) {
    if(FeaturestoreHelper.jobNameGetOrDefault(null) != null){
      jobs.add(FeaturestoreHelper.jobNameGetOrDefault(null));
    }
    List<FeaturestoreJobDTO> jobsDTOs = jobs.stream().map(jobName -> {
      FeaturestoreJobDTO featurestoreJobDTO = new FeaturestoreJobDTO();
      featurestoreJobDTO.setJobName(jobName);
      return featurestoreJobDTO;
    }).collect(Collectors.toList());
    trainingDatasetDTO.setJobs(jobsDTOs);
    return trainingDatasetDTO;
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
  
  public FeaturestoreInsertIntoTrainingDataset setStatColumns(List<String> statColumns) {
    this.statColumns = statColumns;
    return this;
  }
  
}
