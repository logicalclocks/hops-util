package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.Constants;
import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetFormatNotSupportedError;
import io.hops.util.featurestore.dtos.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import io.hops.util.featurestore.dtos.TrainingDatasetDTO;
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
   */
  public Dataset<Row> read()
    throws FeaturestoreNotFound, JAXBException, TrainingDatasetFormatNotSupportedError,
    TrainingDatasetDoesNotExistError, IOException {
    try {
      return doGetTrainingDataset(spark, name, Hops.getFeaturestoreMetadata()
        .setFeaturestore(featurestore).read(), version);
    } catch(Exception e) {
      Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
      return doGetTrainingDataset(spark, name, Hops.getFeaturestoreMetadata()
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
   */
  private Dataset<Row> doGetTrainingDataset(
    SparkSession sparkSession, String trainingDataset,
    FeaturestoreMetadataDTO featurestoreMetadata, int trainingDatasetVersion)
    throws TrainingDatasetDoesNotExistError,
    TrainingDatasetFormatNotSupportedError, IOException {
    sparkSession = FeaturestoreHelper.sparkGetOrDefault(sparkSession);
    List<TrainingDatasetDTO> trainingDatasetDTOList = featurestoreMetadata.getTrainingDatasets();
    TrainingDatasetDTO trainingDatasetDTO = FeaturestoreHelper.findTrainingDataset(trainingDatasetDTOList,
      trainingDataset, trainingDatasetVersion);
    String hdfsPath = Constants.HDFS_DEFAULT + trainingDatasetDTO.getHdfsStorePath() +
      Constants.SLASH_DELIMITER + trainingDatasetDTO.getName();
    return FeaturestoreHelper.getTrainingDataset(sparkSession, trainingDatasetDTO.getDataFormat(),
      hdfsPath);
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
