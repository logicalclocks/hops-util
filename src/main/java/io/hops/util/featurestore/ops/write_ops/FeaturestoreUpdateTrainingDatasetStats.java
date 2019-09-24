package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.FeaturestoreRestClient;
import io.hops.util.Hops;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturegroupUpdateStatsError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.OnlineFeaturestorePasswordNotFound;
import io.hops.util.exceptions.OnlineFeaturestoreUserNotFound;
import io.hops.util.exceptions.SparkDataTypeNotRecognizedError;
import io.hops.util.exceptions.StorageConnectorDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetFormatNotSupportedError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.jobs.FeaturestoreJobDTO;
import io.hops.util.featurestore.dtos.stats.StatisticsDTO;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadTrainingDataset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builder class for Update-TrainingDataset-Stats operation on the Hopsworks Featurestore
 */
public class FeaturestoreUpdateTrainingDatasetStats extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the training dataset to update stats of
   */
  public FeaturestoreUpdateTrainingDatasetStats(String name) {
    super(name);
  }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }
  
  /**
   * Updates the stats of a training dataset
   *
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws IOException IOException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   */
  public void write()
    throws DataframeIsEmpty, SparkDataTypeNotRecognizedError,
    JAXBException, FeaturegroupUpdateStatsError, IOException, FeaturestoreNotFound,
    TrainingDatasetDoesNotExistError, TrainingDatasetFormatNotSupportedError, JWTNotFoundException, HiveNotEnabled,
    StorageConnectorDoesNotExistError, OnlineFeaturestoreUserNotFound, OnlineFeaturestorePasswordNotFound {
    Dataset<Row> sparkDf = new FeaturestoreReadTrainingDataset(name).setSpark(getSpark())
      .setFeaturestore(featurestore).setVersion(version).read();
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(name, getSpark(), sparkDf,
      featurestore, version, descriptiveStats, featureCorr, featureHistograms, clusterAnalysis,
      statColumns, numBins, numClusters, corrMethod);
    FeaturestoreMetadataDTO featurestoreMetadataDTO =
        Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read();
    TrainingDatasetDTO trainingDatasetDTO = FeaturestoreHelper.findTrainingDataset(
        featurestoreMetadataDTO.getTrainingDatasets(), name, version);
    FeaturestoreRestClient.updateTrainingDatasetStatsRest(groupInputParamsIntoDTO(trainingDatasetDTO, statisticsDTO),
      FeaturestoreHelper.getTrainingDatasetDTOTypeStr(trainingDatasetDTO, featurestoreMetadataDTO.getSettings()));
  }
  
  /**
   * Group input parameters into a DTO representation
   *
   * @param trainingDatasetDTO dto to populate
   * @param statisticsDTO statistics computed based on the dataframe
   * @return populated training dataset DTO
   */
  private TrainingDatasetDTO groupInputParamsIntoDTO(TrainingDatasetDTO trainingDatasetDTO,
    StatisticsDTO statisticsDTO) {
    if(FeaturestoreHelper.jobNameGetOrDefault(null) != null){
      jobs.add(FeaturestoreHelper.jobNameGetOrDefault(null));
    }
    List<FeaturestoreJobDTO> jobsDTOs = jobs.stream().map(jobName -> {
      FeaturestoreJobDTO featurestoreJobDTO = new FeaturestoreJobDTO();
      featurestoreJobDTO.setJobName(jobName);
      return featurestoreJobDTO;
    }).collect(Collectors.toList());
    trainingDatasetDTO.setDescriptiveStatistics(statisticsDTO.getDescriptiveStatsDTO());
    trainingDatasetDTO.setFeatureCorrelationMatrix(statisticsDTO.getFeatureCorrelationMatrixDTO());
    trainingDatasetDTO.setFeaturesHistogram(statisticsDTO.getFeatureDistributionsDTO());
    trainingDatasetDTO.setClusterAnalysis(statisticsDTO.getClusterAnalysisDTO());
    trainingDatasetDTO.setJobs(jobsDTOs);
    return trainingDatasetDTO;
  }
  
  public FeaturestoreUpdateTrainingDatasetStats setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreUpdateTrainingDatasetStats setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreUpdateTrainingDatasetStats setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
  
  public FeaturestoreUpdateTrainingDatasetStats setVersion(int version) {
    this.version = version;
    return this;
  }
  
  public FeaturestoreUpdateTrainingDatasetStats setCorrMethod(String corrMethod) {
    this.corrMethod = corrMethod;
    return this;
  }
  
  public FeaturestoreUpdateTrainingDatasetStats setNumBins(int numBins) {
    this.numBins = numBins;
    return this;
  }
  
  public FeaturestoreUpdateTrainingDatasetStats setNumClusters(int numClusters) {
    this.numClusters = numClusters;
    return this;
  }
  
  public FeaturestoreUpdateTrainingDatasetStats setMode(String mode) {
    this.mode = mode;
    return this;
  }
  
  public FeaturestoreUpdateTrainingDatasetStats setDataframe(Dataset<Row> dataframe) {
    this.dataframe = dataframe;
    return this;
  }
  
  public FeaturestoreUpdateTrainingDatasetStats setDescriptiveStats(Boolean descriptiveStats) {
    this.descriptiveStats = descriptiveStats;
    return this;
  }
  
  public FeaturestoreUpdateTrainingDatasetStats setFeatureCorr(Boolean featureCorr) {
    this.featureCorr = featureCorr;
    return this;
  }
  
  public FeaturestoreUpdateTrainingDatasetStats setFeatureHistograms(Boolean featureHistograms) {
    this.featureHistograms = featureHistograms;
    return this;
  }
  
  public FeaturestoreUpdateTrainingDatasetStats setClusterAnalysis(Boolean clusterAnalysis) {
    this.clusterAnalysis = clusterAnalysis;
    return this;
  }
  
  public FeaturestoreUpdateTrainingDatasetStats setStatColumns(List<String> statColumns) {
    this.statColumns = statColumns;
    return this;
  }
  
}
