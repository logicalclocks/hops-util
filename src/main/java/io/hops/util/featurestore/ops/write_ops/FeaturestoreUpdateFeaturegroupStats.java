package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.FeaturestoreRestClient;
import io.hops.util.Hops;
import io.hops.util.exceptions.CannotUpdateStatsOfOnDemandFeaturegroups;
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
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupType;
import io.hops.util.featurestore.dtos.jobs.FeaturestoreJobDTO;
import io.hops.util.featurestore.dtos.stats.StatisticsDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.bind.JAXBException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builder class for Update-Featuregroup-Stats operation on the Hopsworks Featurestore
 */
public class FeaturestoreUpdateFeaturegroupStats extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the featuregroup to update stats of
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   */
  public FeaturestoreUpdateFeaturegroupStats(String name)
    throws FeaturegroupDoesNotExistError, JAXBException, FeaturestoreNotFound {
    super(name);
    // Update metadata cache
    Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
    FeaturestoreMetadataDTO featurestoreMetadata = FeaturestoreHelper.getFeaturestoreMetadataCache();
    FeaturegroupDTO featuregroupDTO = FeaturestoreHelper.findFeaturegroup(featurestoreMetadata.getFeaturegroups(),
      name, version);
    // Get and set previous statistics settings as default
    this.corrMethod = featuregroupDTO.getCorrMethod();
    this.numBins = featuregroupDTO.getNumBins();
    this.numClusters = featuregroupDTO.getNumClusters();
    this.statColumns = featuregroupDTO.getStatisticColumns();
    this.descriptiveStats = featuregroupDTO.isDescStatsEnabled();
    this.clusterAnalysis = featuregroupDTO.isClusterAnalysisEnabled();
    this.featureCorr = featuregroupDTO.isFeatCorrEnabled();
    this.featureHistograms = featuregroupDTO.isFeatHistEnabled();
  }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }
  
  /**
   * Updates the stats of a featuregroup
   *
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws CannotUpdateStatsOfOnDemandFeaturegroups CannotUpdateStatsOfOnDemandFeaturegroups
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  public void write()
    throws DataframeIsEmpty, SparkDataTypeNotRecognizedError,
    JAXBException, FeaturegroupUpdateStatsError, FeaturestoreNotFound, JWTNotFoundException,
    FeaturegroupDoesNotExistError, HiveNotEnabled, CannotUpdateStatsOfOnDemandFeaturegroups,
    OnlineFeaturestoreUserNotFound, OnlineFeaturestorePasswordNotFound, OnlineFeaturestoreNotEnabled {
    FeaturestoreMetadataDTO featurestoreMetadata = FeaturestoreHelper.getFeaturestoreMetadataCache();
    FeaturegroupDTO featuregroupDTO = FeaturestoreHelper.findFeaturegroup(featurestoreMetadata.getFeaturegroups(),
        name, version);
    if(featuregroupDTO.getFeaturegroupType() == FeaturegroupType.ON_DEMAND_FEATURE_GROUP){
      throw new CannotUpdateStatsOfOnDemandFeaturegroups("The update-statistics operation is not supported for " +
          "on-demand feature groups");
    }
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(name, getSpark(), dataframe,
      featurestore, version, descriptiveStats, featureCorr, featureHistograms, clusterAnalysis,
      statColumns, numBins, numClusters, corrMethod);
    FeaturestoreRestClient.updateFeaturegroupStatsRest(groupInputParamsIntoDTO(featuregroupDTO, statisticsDTO),
      FeaturestoreHelper.getFeaturegroupDtoTypeStr(featurestoreMetadata.getSettings(), false));
  }
  
  /**
   * Groups input parameters into a DTO representation
   *
   * @param statisticsDTO statistics computed based on the dataframe
   * @return FeaturegroupDTO
   */
  private FeaturegroupDTO groupInputParamsIntoDTO(FeaturegroupDTO featuregroupDTO, StatisticsDTO statisticsDTO){
    if(FeaturestoreHelper.jobNameGetOrDefault(null) != null){
      jobs.add(FeaturestoreHelper.jobNameGetOrDefault(null));
    }
    List<FeaturestoreJobDTO> jobsDTOs = jobs.stream().map(jobName -> {
      FeaturestoreJobDTO featurestoreJobDTO = new FeaturestoreJobDTO();
      featurestoreJobDTO.setJobName(jobName);
      return featurestoreJobDTO;
    }).collect(Collectors.toList());
    featuregroupDTO.setDescriptiveStatistics(statisticsDTO.getDescriptiveStatsDTO());
    featuregroupDTO.setFeatureCorrelationMatrix(statisticsDTO.getFeatureCorrelationMatrixDTO());
    featuregroupDTO.setFeaturesHistogram(statisticsDTO.getFeatureDistributionsDTO());
    featuregroupDTO.setClusterAnalysis(statisticsDTO.getClusterAnalysisDTO());
    featuregroupDTO.setJobs(jobsDTOs);
    featuregroupDTO.setClusterAnalysisEnabled(clusterAnalysis);
    featuregroupDTO.setFeatCorrEnabled(featureCorr);
    featuregroupDTO.setFeatHistEnabled(featureHistograms);
    featuregroupDTO.setDescStatsEnabled(descriptiveStats);
    featuregroupDTO.setNumBins(numBins);
    featuregroupDTO.setNumClusters(numClusters);
    featuregroupDTO.setCorrMethod(corrMethod);
    featuregroupDTO.setStatisticColumns(statColumns);
    return featuregroupDTO;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setVersion(int version) {
    this.version = version;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setCorrMethod(String corrMethod) {
    this.corrMethod = corrMethod;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setNumBins(int numBins) {
    this.numBins = numBins;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setNumClusters(int numClusters) {
    this.numClusters = numClusters;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setMode(String mode) {
    this.mode = mode;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setDataframe(Dataset<Row> dataframe) {
    this.dataframe = dataframe;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setDescriptiveStats(Boolean descriptiveStats) {
    this.descriptiveStats = descriptiveStats;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setFeatureCorr(Boolean featureCorr) {
    this.featureCorr = featureCorr;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setFeatureHistograms(Boolean featureHistograms) {
    this.featureHistograms = featureHistograms;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setClusterAnalysis(Boolean clusterAnalysis) {
    this.clusterAnalysis = clusterAnalysis;
    return this;
  }
  
  public FeaturestoreUpdateFeaturegroupStats setStatColumns(List<String> statColumns) {
    this.statColumns = statColumns;
    return this;
  }
  
}
