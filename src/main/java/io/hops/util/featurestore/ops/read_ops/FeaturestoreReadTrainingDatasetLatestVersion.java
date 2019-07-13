package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;

import javax.xml.bind.JAXBException;
import java.util.List;

/**
 * Builder class for Read-LatestTrainingDatasetVersion operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadTrainingDatasetLatestVersion extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the training dataset
   */
  public FeaturestoreReadTrainingDatasetLatestVersion(String name) {
    super(name);
  }
  
  /**
   * Gets the latest version of a training dataset in the featurestore
   *
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @return the latest version of the training dataset
   */
  public Integer read() throws FeaturestoreNotFound, JAXBException {
    try {
      return doGetLatestTrainingDatasetVersion(name, Hops.getFeaturestoreMetadata()
        .setFeaturestore(featurestore).read());
    } catch (Exception e) {
      Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
      return doGetLatestTrainingDatasetVersion(name,
        Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read());
    }
  }
  
  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
  public FeaturestoreReadTrainingDatasetLatestVersion setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  /**
   * Gets the latest version of a training dataset in the feature store, returns 0 if no version exists
   *
   * @param trainingDatasetName the name of the trainingDataset to get the latest version of
   * @param featurestoreMetadata metadata of the featurestore to query
   * @return the latest version of the training dataset
   */
  private static int doGetLatestTrainingDatasetVersion(
    String trainingDatasetName, FeaturestoreMetadataDTO featurestoreMetadata) {
    List<TrainingDatasetDTO> trainingDatasetDTOS = featurestoreMetadata.getTrainingDatasets();
    return FeaturestoreHelper.getLatestTrainingDatasetVersion(trainingDatasetDTOS, trainingDatasetName);
  }
  
}
