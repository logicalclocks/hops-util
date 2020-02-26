package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.StorageConnectorDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
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
    int trainingDatasetVersion, FeaturestoreMetadataDTO featurestoreMetadata) throws TrainingDatasetDoesNotExistError {

    List<TrainingDatasetDTO> trainingDatasetDTOList = featurestoreMetadata.getTrainingDatasets();
    TrainingDatasetDTO trainingDatasetDTO = FeaturestoreHelper.findTrainingDataset(trainingDatasetDTOList,
      trainingDataset, trainingDatasetVersion);

    if(trainingDatasetDTO.getTrainingDatasetType() == TrainingDatasetType.HOPSFS_TRAINING_DATASET){
      return FeaturestoreHelper.getHopsfsTrainingDatasetPath(trainingDatasetDTO);
    } else {
      return FeaturestoreHelper.getExternalTrainingDatasetPath(trainingDatasetDTO);
    }
  }

}
