package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.StorageConnectorDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreS3ConnectorDTO;
import io.hops.util.featurestore.dtos.trainingdataset.ExternalTrainingDatasetDTO;
import io.hops.util.featurestore.dtos.trainingdataset.HopsfsTrainingDatasetDTO;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetDTO;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetType;
import io.hops.util.featurestore.ops.FeaturestoreOp;

import javax.xml.bind.JAXBException;
import java.util.List;

/**
 * Builder class for Read-TrainingDatasetPath operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadTrainingDatasetPath extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name the name of the training dataset
   */
  public FeaturestoreReadTrainingDatasetPath(String name) {
    super(name);
  }
  
  /**
   * Gets the training dataset HDFS path
   *
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @return the HDFS path to the training dataset
   */
  public String read()
      throws FeaturestoreNotFound, JAXBException, TrainingDatasetDoesNotExistError, StorageConnectorDoesNotExistError {
    try {
      return doGetTrainingDatasetPath(name, version,
        Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read());
    } catch (Exception e) {
      Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
      return doGetTrainingDatasetPath(name, version,
        Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read());
    }
  }
  
  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
  public FeaturestoreReadTrainingDatasetPath setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreReadTrainingDatasetPath setVersion(int version) {
    this.version = version;
    return this;
  }
  
  /**
   * Gets the HDFS path to a training dataset with a specific name and version in a featurestore
   *
   * @param trainingDataset        name of the training dataset
   * @param featurestoreMetadata   featurestore metadata
   * @param trainingDatasetVersion version of the training dataset
   * @return the hdfs path to the training dataset
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   */
  private static String doGetTrainingDatasetPath(String trainingDataset,
    int trainingDatasetVersion,
    FeaturestoreMetadataDTO featurestoreMetadata) throws
      TrainingDatasetDoesNotExistError, StorageConnectorDoesNotExistError {
    List<TrainingDatasetDTO> trainingDatasetDTOList = featurestoreMetadata.getTrainingDatasets();
    TrainingDatasetDTO trainingDatasetDTO = FeaturestoreHelper.findTrainingDataset(trainingDatasetDTOList,
      trainingDataset, trainingDatasetVersion);
    if(trainingDatasetDTO.getTrainingDatasetType() == TrainingDatasetType.HOPSFS_TRAINING_DATASET){
      return doGetHopsfsTrainingDatasetPath(trainingDatasetDTO);
    } else {
      return doGetExternalTrainingDatasetPath(trainingDatasetDTO, featurestoreMetadata);
    }
  }
  
  /**
   * Gets the path of a hopsfs training dataset
   *
   * @param trainingDatasetDTO DTO information about the training dataset
   * @return the HopsFS path
   */
  private static String doGetHopsfsTrainingDatasetPath(TrainingDatasetDTO trainingDatasetDTO) {
    HopsfsTrainingDatasetDTO hopsfsTrainingDatasetDTO = (HopsfsTrainingDatasetDTO) trainingDatasetDTO;
    return FeaturestoreHelper.getHopsfsTrainingDatasetPath(hopsfsTrainingDatasetDTO);
  }
  
  /**
   * Gets the path to an external training dataset (e.g S3 path)
   *
   * @param trainingDatasetDTO DTO information about the training dataset
   * @param featurestoreMetadataDTO metadata about the feature store
   * @return the external training dataset path
   * @throws StorageConnectorDoesNotExistError
   */
  private static String doGetExternalTrainingDatasetPath(TrainingDatasetDTO trainingDatasetDTO,
    FeaturestoreMetadataDTO featurestoreMetadataDTO) throws StorageConnectorDoesNotExistError {
    ExternalTrainingDatasetDTO externalTrainingDatasetDTO = (ExternalTrainingDatasetDTO) trainingDatasetDTO;
    FeaturestoreS3ConnectorDTO s3ConnectorDTO = (FeaturestoreS3ConnectorDTO) FeaturestoreHelper.findStorageConnector(
      featurestoreMetadataDTO.getStorageConnectors(), externalTrainingDatasetDTO.getS3ConnectorName());
    String path = FeaturestoreHelper.getExternalTrainingDatasetPath(externalTrainingDatasetDTO.getName(),
      externalTrainingDatasetDTO.getVersion(), s3ConnectorDTO.getBucket());
    return path;
  }
  
}
