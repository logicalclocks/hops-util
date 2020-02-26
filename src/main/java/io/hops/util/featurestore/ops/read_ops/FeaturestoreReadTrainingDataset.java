package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.exceptions.StorageConnectorDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetFormatNotSupportedError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreS3ConnectorDTO;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetDTO;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetType;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;

/**
 * Builder class for Read-TrainingDataset operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadTrainingDataset extends FeaturestoreOp {

  /**
   * Constructor
   *
   * @param name name of the training dataset to read
   */
  public FeaturestoreReadTrainingDataset(String name) {
    super(name);
  }

  /**
   * Gets a training dataset from a particular featurestore
   *
   * @return a spark dataframe with the training dataset
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws IOException IOException
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   */
  public Dataset<Row> read()
      throws FeaturestoreNotFound, JAXBException, TrainingDatasetFormatNotSupportedError,
      TrainingDatasetDoesNotExistError, IOException, HiveNotEnabled, StorageConnectorDoesNotExistError {
    try {
      return doGetTrainingDataset(getSpark(), name, Hops.getFeaturestoreMetadata()
        .setFeaturestore(featurestore).read(), version);
    } catch(Exception e) {
      Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
      return doGetTrainingDataset(getSpark(), name, Hops.getFeaturestoreMetadata()
        .setFeaturestore(featurestore).read(), version);
    }
  }

  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }

  /**
   * Gets a training dataset from a featurestore
   *
   * @param sparkSession           the spark session
   * @param trainingDataset        the training dataset to get
   * @param featurestoreMetadata   metadata of the featurestore to query
   * @param trainingDatasetVersion the version of the training dataset
   * @return a spark dataframe with the training dataset
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws IOException IOException
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   */
  private Dataset<Row> doGetTrainingDataset(
      SparkSession sparkSession, String trainingDataset,
      FeaturestoreMetadataDTO featurestoreMetadata, int trainingDatasetVersion)
    throws TrainingDatasetDoesNotExistError,
    TrainingDatasetFormatNotSupportedError, IOException, StorageConnectorDoesNotExistError {
    sparkSession = FeaturestoreHelper.sparkGetOrDefault(sparkSession);
    List<TrainingDatasetDTO> trainingDatasetDTOList = featurestoreMetadata.getTrainingDatasets();
    TrainingDatasetDTO trainingDatasetDTO = FeaturestoreHelper.findTrainingDataset(trainingDatasetDTOList,
      trainingDataset, trainingDatasetVersion);
    if(trainingDatasetDTO.getTrainingDatasetType() == TrainingDatasetType.HOPSFS_TRAINING_DATASET){
      return doGetHopsfsTrainingDataset(trainingDatasetDTO, sparkSession);
    } else {
      return doGetExternalTrainingDataset(trainingDatasetDTO, sparkSession, featurestoreMetadata);
    }
  }
  
  /**
   * Reads a HopsFS training Dataset
   *
   * @param trainingDatasetDTO DTO of the training dataset to read
   * @param sparkSession the spark session to use when reading ( the dataset is read from Hopsfs using Spark)
   * @return a dataframe with the training dataset
   * @throws TrainingDatasetFormatNotSupportedError
   * @throws TrainingDatasetDoesNotExistError
   * @throws IOException
   */
  private Dataset<Row> doGetHopsfsTrainingDataset(TrainingDatasetDTO trainingDatasetDTO, SparkSession sparkSession)
    throws TrainingDatasetFormatNotSupportedError, TrainingDatasetDoesNotExistError, IOException {
    String path = FeaturestoreHelper.getHopsfsTrainingDatasetPath(trainingDatasetDTO);
    return FeaturestoreHelper.getTrainingDataset(sparkSession, trainingDatasetDTO.getDataFormat(),
      path, TrainingDatasetType.HOPSFS_TRAINING_DATASET);
  }
  
  /**
   * Reads an external training dataset
   *
   * @param trainingDatasetDTO DTO of the training dataset
   * @param sparkSession the sparksession (spark is used to read the dataset)
   * @param featurestoreMetadataDTO featurestore metadata
   * @return Spark dataframe with the training dataset
   * @throws StorageConnectorDoesNotExistError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws IOException IOException
   */
  private Dataset<Row> doGetExternalTrainingDataset(
    TrainingDatasetDTO trainingDatasetDTO, SparkSession sparkSession, FeaturestoreMetadataDTO featurestoreMetadataDTO)
    throws StorageConnectorDoesNotExistError, TrainingDatasetFormatNotSupportedError,
    TrainingDatasetDoesNotExistError, IOException {

    FeaturestoreS3ConnectorDTO s3ConnectorDTO = (FeaturestoreS3ConnectorDTO) FeaturestoreHelper.findStorageConnector(
      featurestoreMetadataDTO.getStorageConnectors(), trainingDatasetDTO.getStorageConnectorName());
    
    String path = FeaturestoreHelper.getExternalTrainingDatasetPath(trainingDatasetDTO);
    
    FeaturestoreHelper.setupS3CredentialsForSpark(s3ConnectorDTO.getAccessKey(), s3ConnectorDTO.getSecretKey(),
      sparkSession);
    return FeaturestoreHelper.getTrainingDataset(sparkSession, trainingDatasetDTO.getDataFormat(), path,
      TrainingDatasetType.EXTERNAL_TRAINING_DATASET);
  }
  

  public FeaturestoreReadTrainingDataset setName(String name) {
    this.name = name;
    return this;
  }

  public FeaturestoreReadTrainingDataset setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }

  public FeaturestoreReadTrainingDataset setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }

  public FeaturestoreReadTrainingDataset setVersion(int version) {
    this.version = version;
    return this;
  }

}
