package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.Constants;
import io.hops.util.FeaturestoreRestClient;
import io.hops.util.Hops;
import io.hops.util.exceptions.CannotInsertIntoOnDemandFeaturegroups;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturegroupCreationError;
import io.hops.util.exceptions.FeaturegroupDeletionError;
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
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.featuregroup.CachedFeaturegroupDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupType;
import io.hops.util.featurestore.dtos.jobs.FeaturestoreJobDTO;
import io.hops.util.featurestore.dtos.stats.StatisticsDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.bind.JAXBException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Builder class for InsertInto-Featuregroup operation on the Hopsworks Featurestore
 */
public class FeaturestoreInsertIntoFeaturegroup extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the featuregroup to insert into
   */
  public FeaturestoreInsertIntoFeaturegroup(String name) {
    super(name);
  }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }
  
  /**
   * Inserts a dataframes with rows into a featuregroup
   *
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws FeaturegroupDeletionError FeaturegroupDeletionError
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws CannotInsertIntoOnDemandFeaturegroups CannotInsertIntoOnDemandFeaturegroups
   * @throws FeaturegroupCreationError FeaturegroupCreationError
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  public void write()
    throws DataframeIsEmpty, SparkDataTypeNotRecognizedError,
    JAXBException, FeaturegroupUpdateStatsError, FeaturestoreNotFound, JWTNotFoundException,
    FeaturegroupDeletionError, FeaturegroupDoesNotExistError, HiveNotEnabled, CannotInsertIntoOnDemandFeaturegroups,
    StorageConnectorDoesNotExistError, FeaturegroupCreationError, OnlineFeaturestoreUserNotFound,
    OnlineFeaturestorePasswordNotFound, OnlineFeaturestoreNotEnabled {
    FeaturestoreMetadataDTO featurestoreMetadata = FeaturestoreHelper.getFeaturestoreMetadataCache();
    FeaturegroupDTO featuregroupDTO = FeaturestoreHelper.findFeaturegroup(featurestoreMetadata.getFeaturegroups(),
        name, version);
    if(featuregroupDTO.getFeaturegroupType() == FeaturegroupType.ON_DEMAND_FEATURE_GROUP){
      throw new CannotInsertIntoOnDemandFeaturegroups(
          "The insert operation is only supported for cached feature groups");
    }
    FeaturestoreHelper.validateDataframe(dataframe);
    getSpark().sparkContext().setJobGroup(
      "Inserting dataframe into featuregroup",
      "Inserting into featuregroup:" + name + " in the featurestore:" +
        featurestore, true);
    FeaturestoreHelper.validateWriteMode(mode);
    if (mode.equalsIgnoreCase("overwrite")) {
      FeaturestoreRestClient.deleteTableContentsRest(groupInputParamsIntoDTO());
      //update cache because in the background, clearing featuregroup will give it a new id
      new FeaturestoreUpdateMetadataCache().setFeaturestore(featurestore).write();
    }
    doInsertIntoFeaturegroup(Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read());
  }
  
  /**
   * Groups input parameters into a DTO representation
   *
   * @return FeaturegroupDTO
   */
  private FeaturegroupDTO groupInputParamsIntoDTO(){
    if(FeaturestoreHelper.jobNameGetOrDefault(null) != null){
      jobs.add(FeaturestoreHelper.jobNameGetOrDefault(null));
    }
    List<FeaturestoreJobDTO> jobsDTOs = jobs.stream().map(jobName -> {
      FeaturestoreJobDTO featurestoreJobDTO = new FeaturestoreJobDTO();
      featurestoreJobDTO.setJobName(jobName);
      return featurestoreJobDTO;
    }).collect(Collectors.toList());
    FeaturegroupDTO featuregroupDTO = new FeaturegroupDTO();
    featuregroupDTO.setFeaturestoreName(featurestore);
    featuregroupDTO.setName(name);
    featuregroupDTO.setVersion(version);
    featuregroupDTO.setJobs(jobsDTOs);
    return featuregroupDTO;
  }
  
  /**
   * Groups input parameters into a DTO representation
   *
   * @param statisticsDTO statistics computed based on the dataframe
   * @return FeaturegroupDTO
   */
  private FeaturegroupDTO groupInputParamsIntoDTO(StatisticsDTO statisticsDTO){
    if(FeaturestoreHelper.jobNameGetOrDefault(null) != null){
      jobs.add(FeaturestoreHelper.jobNameGetOrDefault(null));
    }
    List<FeaturestoreJobDTO> jobsDTOs = jobs.stream().map(jobName -> {
      FeaturestoreJobDTO featurestoreJobDTO = new FeaturestoreJobDTO();
      featurestoreJobDTO.setJobName(jobName);
      return featurestoreJobDTO;
    }).collect(Collectors.toList());
    FeaturegroupDTO featuregroupDTO = new FeaturegroupDTO();
    featuregroupDTO.setFeaturestoreName(featurestore);
    featuregroupDTO.setName(name);
    featuregroupDTO.setVersion(version);
    featuregroupDTO.setDescriptiveStatistics(statisticsDTO.getDescriptiveStatsDTO());
    featuregroupDTO.setFeatureCorrelationMatrix(statisticsDTO.getFeatureCorrelationMatrixDTO());
    featuregroupDTO.setFeaturesHistogram(statisticsDTO.getFeatureDistributionsDTO());
    featuregroupDTO.setClusterAnalysis(statisticsDTO.getClusterAnalysisDTO());
    featuregroupDTO.setJobs(jobsDTOs);
    return featuregroupDTO;
  }
  
  /**
   * Inserts data into a cached feature group
   *
   * @param featurestoreMetadataDTO metadata about the feature store
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws JAXBException JAXBException
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws FeaturegroupCreationError FeaturegroupCreationError
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  private void doInsertIntoFeaturegroup(FeaturestoreMetadataDTO featurestoreMetadataDTO)
    throws HiveNotEnabled, FeaturestoreNotFound, DataframeIsEmpty, SparkDataTypeNotRecognizedError,
    FeaturegroupDoesNotExistError, FeaturegroupUpdateStatsError, JAXBException, JWTNotFoundException,
    StorageConnectorDoesNotExistError, FeaturegroupCreationError, OnlineFeaturestoreUserNotFound,
    OnlineFeaturestorePasswordNotFound, OnlineFeaturestoreNotEnabled {
    FeaturegroupDTO featuregroupDTO;
    try {
      featuregroupDTO = FeaturestoreHelper.findFeaturegroup(featurestoreMetadataDTO.getFeaturegroups(),name, version);
    } catch(Exception e) {
      Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
      featurestoreMetadataDTO = Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read();
      featuregroupDTO = FeaturestoreHelper.findFeaturegroup(featurestoreMetadataDTO.getFeaturegroups(),name, version);
    }
    getSpark().sparkContext().setJobGroup("", "", true);
    if(featuregroupDTO.getFeaturegroupType() == FeaturegroupType.CACHED_FEATURE_GROUP &&
      ((CachedFeaturegroupDTO) featuregroupDTO).getInputFormat().equalsIgnoreCase(Constants.HUDI_INPUT_FORMAT)) {
      Map<String, String> hudiWriteArgs = setupHudiArgs();
      FeaturestoreHelper.writeHudiDataset(dataframe, getSpark(), name, featurestore, version,
        hudiWriteArgs, ((CachedFeaturegroupDTO) featuregroupDTO).getHdfsStorePaths().get(0), mode);
      StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(name, getSpark(), dataframe,
        featurestore, version,
        descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns, numBins, numClusters,
        corrMethod);
      new FeaturestoreSyncHiveTable(name).setFeaturestore(featurestore).setDescription(featuregroupDTO.getDescription())
        .setVersion(featuregroupDTO.getVersion()).setStatisticsDTO(statisticsDTO).setJobs(jobs).write();
    } else {
      if(offline) {
        FeaturestoreHelper.insertIntoOfflineFeaturegroup(dataframe, getSpark(), name,
          featurestore, version);
      }
      if (online) {
        FeaturestoreHelper.insertIntoOnlineFeaturegroup(dataframe, name, featurestore, version, mode);
      }
      StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(name, getSpark(), dataframe,
        featurestore, version, descriptiveStats, featureCorr, featureHistograms, clusterAnalysis,
        statColumns, numBins, numClusters, corrMethod);
      Boolean onDemand = featuregroupDTO.getFeaturegroupType() == FeaturegroupType.ON_DEMAND_FEATURE_GROUP;
      FeaturestoreRestClient.updateFeaturegroupStatsRest(groupInputParamsIntoDTO(statisticsDTO),
        FeaturestoreHelper.getFeaturegroupDtoTypeStr(featurestoreMetadataDTO.getSettings(), onDemand));
    }
    getSpark().sparkContext().setJobGroup("", "", true);
  }
  
  /**
   * Setup the Hudi arguments for doing an upsert into a hudi feature group
   *
   * @return the hudi write arguments
   * @throws StorageConnectorDoesNotExistError
   */
  private Map<String, String> setupHudiArgs() throws StorageConnectorDoesNotExistError {
    List<String> primaryKeys = FeaturestoreHelper.primaryKeyGetOrDefault(getPrimaryKeys(), dataframe);
    //Add default args
    Map<String, String> hArgs = Constants.HUDI_DEFAULT_ARGS;
    hArgs.put(Constants.HUDI_TABLE_OPERATION, Constants.HUDI_UPSERT);
    hArgs.put(Constants.HUDI_TABLE_NAME, FeaturestoreHelper.getTableName(name, version));
    hArgs.put(Constants.HUDI_RECORD_KEY, primaryKeys.get(0));
    if(!partitionBy.isEmpty()) {
      hArgs.put(Constants.HUDI_PARTITION_FIELD, StringUtils.join(partitionBy, ","));
      hArgs.put(Constants.HUDI_PRECOMBINE_FIELD, StringUtils.join(partitionBy, ","));
      hArgs.put(Constants.HUDI_HIVE_SYNC_PARTITION_FIELDS, StringUtils.join(partitionBy, ","));
    }
    hArgs = FeaturestoreHelper.setupHudiHiveArgs(hArgs, FeaturestoreHelper.getTableName(name, version));
    
    //Add User-supplied args
    for (Map.Entry<String, String> entry : hudiArgs.entrySet()) {
      hArgs.put(entry.getKey(), entry.getValue());
    }
    
    return hArgs;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setVersion(int version) {
    this.version = version;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setCorrMethod(String corrMethod) {
    this.corrMethod = corrMethod;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setNumBins(int numBins) {
    this.numBins = numBins;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setNumClusters(int numClusters) {
    this.numClusters = numClusters;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setMode(String mode) {
    this.mode = mode;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setDataframe(Dataset<Row> dataframe) {
    this.dataframe = dataframe;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setDescriptiveStats(Boolean descriptiveStats) {
    this.descriptiveStats = descriptiveStats;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setFeatureCorr(Boolean featureCorr) {
    this.featureCorr = featureCorr;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setFeatureHistograms(Boolean featureHistograms) {
    this.featureHistograms = featureHistograms;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setClusterAnalysis(Boolean clusterAnalysis) {
    this.clusterAnalysis = clusterAnalysis;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setStatColumns(List<String> statColumns) {
    this.statColumns = statColumns;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setHudi(Boolean hudi) {
    this.hudi = hudi;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setHudiArgs(Map<String, String> hudiArgs) {
    this.hudiArgs = hudiArgs;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setHudiBasePath(String hudiBasePath) {
    this.hudiBasePath = hudiBasePath;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setPartitionBy(List<String> partitionBy) {
    this.partitionBy = partitionBy;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setPrimaryKeys(List<String> primaryKeys) {
    this.primaryKeys = primaryKeys;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setOnline(Boolean online) {
    this.online = online;
    return this;
  }
  
  public FeaturestoreInsertIntoFeaturegroup setOffline(Boolean offline) {
    this.offline = offline;
    return this;
  }
  
}
