package io.hops.util.featurestore.ops.write_ops;

import com.uber.hoodie.DataSourceUtils;
import com.uber.hoodie.DataSourceWriteOptions;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.hadoop.HoodieInputFormat;
import com.uber.hoodie.hive.HiveSyncConfig;
import com.uber.hoodie.hive.HiveSyncTool;
import com.uber.hoodie.hive.HoodieHiveClient;
import com.uber.hoodie.hive.MultiPartKeysValueExtractor;
import com.uber.hoodie.hive.util.SchemaUtil;
import io.hops.util.FeaturestoreRestClient;
import io.hops.util.Hops;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturegroupCreationError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.exceptions.InvalidPrimaryKeyForFeaturegroup;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.SparkDataTypeNotRecognizedError;
import io.hops.util.exceptions.StorageConnectorDoesNotExistError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.feature.FeatureDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupType;
import io.hops.util.featurestore.dtos.featuregroup.OnDemandFeaturegroupDTO;
import io.hops.util.featurestore.dtos.jobs.FeaturestoreJobDTO;
import io.hops.util.featurestore.dtos.stats.StatisticsDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreStorageConnectorDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreStorageConnectorType;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.parquet.Strings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import parquet.schema.MessageType;


import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.logging.Logger;

/**
 * Builder class for Create-Featuregroup operation on the Hopsworks Featurestore
 */
public class FeaturestoreCreateFeaturegroup extends FeaturestoreOp {

  private static final Logger LOG = Logger.getLogger(FeaturestoreCreateFeaturegroup.class.getName());
  
  /**
   * Constructor
   *
   * @param name name of the featuregroup to create
   */
  public FeaturestoreCreateFeaturegroup(String name) {
    super(name);
  }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }

  /**
   * Creates a new feature group in the featurestore
   *
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws JAXBException JAXBException
   * @throws InvalidPrimaryKeyForFeaturegroup InvalidPrimaryKeyForFeaturegroup
   * @throws FeaturegroupCreationError FeaturegroupCreationError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JWTNotFoundException JWTNotFounfdException
   * @throws IOException IOException
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   */
  public void write() throws JWTNotFoundException, FeaturegroupCreationError, SparkDataTypeNotRecognizedError,
      FeaturestoreNotFound, IOException, JAXBException, InvalidPrimaryKeyForFeaturegroup, HiveNotEnabled, DataframeIsEmpty,
      StorageConnectorDoesNotExistError {
    if(onDemand){
      writeOnDemandFeaturegroup();
    } else {
      writeCachedFeaturegroup();
    }
    //Update metadata cache since we created a new feature group
    Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
  }

  /**
   * Creates a new onDemand Featuregroup in teh Feature store
   *
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws FeaturegroupCreationError FeaturegroupCreationError
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws JAXBException JAXBException
   */
  public void writeOnDemandFeaturegroup() throws StorageConnectorDoesNotExistError, FeaturestoreNotFound,
      FeaturegroupCreationError, JWTNotFoundException, JAXBException {
    if(Strings.isNullOrEmpty(sqlQuery)){
      throw new IllegalArgumentException("SQL Query Cannot be Empty or Null for On-Demand Feature Groups");
    }
    if(Strings.isNullOrEmpty(jdbcConnector)){
      throw new IllegalArgumentException("To create an on-demand feature group you must specify the name of a " +
          "JDBC Storage Connector");
    }
    FeaturestoreMetadataDTO featurestoreMetadata = FeaturestoreHelper.getFeaturestoreMetadataCache();
    FeaturestoreStorageConnectorDTO storageConnectorDTO = FeaturestoreHelper.findStorageConnector(
        featurestoreMetadata.getStorageConnectors(), jdbcConnector);
    if(storageConnectorDTO.getStorageConnectorType() != FeaturestoreStorageConnectorType.JDBC){
      throw new IllegalArgumentException("OnDemand Feature groups can only be linked to JDBC Storage Connectors, " +
          "the provided storage connector is of type: " + storageConnectorDTO.getStorageConnectorType());
    }

    FeaturestoreRestClient.createFeaturegroupRest(groupInputParamsIntoDTO(storageConnectorDTO.getId()),
        FeaturestoreHelper.getFeaturegroupDtoTypeStr(featurestoreMetadata.getSettings(), onDemand), null);
  }

  /**
   * Creates a new cached feature group in the featurestore
   *
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws JAXBException JAXBException
   * @throws InvalidPrimaryKeyForFeaturegroup InvalidPrimaryKeyForFeaturegroup
   * @throws FeaturegroupCreationError FeaturegroupCreationError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JWTNotFoundException JWTNotFounfdException
   * @throws IOException IOException
   * @throws HiveNotEnabled HiveNotEnabled
   */
  public void writeCachedFeaturegroup()
    throws DataframeIsEmpty, SparkDataTypeNotRecognizedError,
    JAXBException, InvalidPrimaryKeyForFeaturegroup, FeaturegroupCreationError, FeaturestoreNotFound,
    JWTNotFoundException, IOException,  HiveNotEnabled {
    if(dataframe == null) {
      throw new IllegalArgumentException("Dataframe to create featuregroup from cannot be null, specify dataframe " +
        "with " +
        ".setDataframe(df)");
    }
    primaryKey = FeaturestoreHelper.primaryKeyGetOrDefault(primaryKey, dataframe);
    FeaturestoreHelper.validatePrimaryKey(dataframe, primaryKey);
    FeaturestoreHelper.validateMetadata(name, dataframe.dtypes(), description);
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(name, getSpark(), dataframe,
      featurestore, version, descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns,
      numBins, numClusters, corrMethod);
    List<FeatureDTO> featuresSchema = FeaturestoreHelper.parseSparkFeaturesSchema(dataframe.schema(), primaryKey,
      partitionBy);
    FeaturestoreMetadataDTO featurestoreMetadata = FeaturestoreHelper.getFeaturestoreMetadataCache();

    String createTableSql = null;
    HiveSyncTool hiveSyncTool = null;
    if(hudi){
      String tableName = FeaturestoreHelper.getTableName(name, version);
      FeaturestoreHelper.hoodieTable(dataframe, hudiArgs, hudiTableBasePath, tableName);
      hiveSyncTool = buildHiveSyncTool(tableName);
      createTableSql = getHudiTableDDLSql(hiveSyncTool);
    }
    FeaturestoreRestClient.createFeaturegroupRest(groupInputParamsIntoDTO(featuresSchema, statisticsDTO),
      FeaturestoreHelper.getFeaturegroupDtoTypeStr(featurestoreMetadata.getSettings(), onDemand), createTableSql);
    FeaturestoreHelper.insertIntoFeaturegroup(dataframe, getSpark(), name,
          featurestore, version, hudi, hudiArgs, hudiTableBasePath, hiveSyncTool);
  }

  private HiveSyncTool buildHiveSyncTool(String tableName) throws HiveNotEnabled{
    TypedProperties props = new TypedProperties();
    props.put(DataSourceWriteOptions.HIVE_ASSUME_DATE_PARTITION_OPT_KEY(),
          Boolean.valueOf(DataSourceWriteOptions.DEFAULT_HIVE_ASSUME_DATE_PARTITION_OPT_VAL()));
    props.put(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY(), featurestore);
    props.put(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), tableName);
    props.put(DataSourceWriteOptions.HIVE_USER_OPT_KEY(), hudiArgs.get(DataSourceWriteOptions.HIVE_USER_OPT_KEY()));
    props.put(DataSourceWriteOptions.HIVE_PASS_OPT_KEY(), hudiArgs.get(DataSourceWriteOptions.HIVE_PASS_OPT_KEY()));
    props.put(DataSourceWriteOptions.HIVE_URL_OPT_KEY(), hudiArgs.get(DataSourceWriteOptions.HIVE_URL_OPT_KEY()));
    props.put(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(),
          hudiArgs.get(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY()));
    props.put(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY(),
          MultiPartKeysValueExtractor.class.getName());


    HiveConf hiveConf = new HiveConf(true);
    hiveConf.addResource(getSpark().sparkContext().hadoopConfiguration());
    HiveSyncConfig hiveSyncConfig = DataSourceUtils.buildHiveSyncConfig(props, hudiTableBasePath);
    FileSystem fs = FSUtils.getFs(hudiTableBasePath, getSpark().sparkContext().hadoopConfiguration());
    HiveSyncTool hiveSyncTool = new HiveSyncTool(hiveSyncConfig, hiveConf, fs);
    return hiveSyncTool;
  }

  private String getHudiTableDDLSql(HiveSyncTool hiveSyncTool) throws IOException {
    //Get the HoodieHiveClient
    HoodieHiveClient hoodieHiveClient = hiveSyncTool.getHoodieHiveClient();

    // Get the parquet schema for this dataset looking at the latest commit
    MessageType schema = hoodieHiveClient.getDataSchema();
    HiveSyncConfig cfg = hiveSyncTool.getSyncCfg();
    String createSQLQuery = SchemaUtil.generateCreateDDL(schema, cfg, HoodieInputFormat.class.getName(),
          MapredParquetOutputFormat.class.getName(), ParquetHiveSerDe.class.getName());
    return createSQLQuery;
  }

  /**
   * Group input parameters into a DTO for creating an on-demand feature group
   *
   * @param jdbcConnectorId id of the jdbc connector to get the on-demand feature group from
   * @return DTO representation of the input parameters
   */
  private FeaturegroupDTO groupInputParamsIntoDTO(Integer jdbcConnectorId) {
    OnDemandFeaturegroupDTO onDemandFeaturegroupDTO = new OnDemandFeaturegroupDTO();
    onDemandFeaturegroupDTO.setFeaturestoreName(featurestore);
    onDemandFeaturegroupDTO.setName(name);
    onDemandFeaturegroupDTO.setVersion(version);
    onDemandFeaturegroupDTO.setDescription(description);
    onDemandFeaturegroupDTO.setQuery(sqlQuery);
    onDemandFeaturegroupDTO.setJdbcConnectorId(jdbcConnectorId);
    onDemandFeaturegroupDTO.setFeaturegroupType(FeaturegroupType.ON_DEMAND_FEATURE_GROUP);
    return onDemandFeaturegroupDTO;
  }
  
  /**
   * Group input parameters into a DTO for creating a cached feature group
   *
   * @param features feature schema (inferred from the dataframe)
   * @param statisticsDTO statisticsDTO (computed based on the dataframe)
   * @return DTO representation of the input parameters
   */
  private FeaturegroupDTO groupInputParamsIntoDTO(List<FeatureDTO> features, StatisticsDTO statisticsDTO){
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
    featuregroupDTO.setDescription(description);
    featuregroupDTO.setJobs(jobsDTOs);
    featuregroupDTO.setFeatures(features);
    featuregroupDTO.setClusterAnalysis(statisticsDTO.getClusterAnalysisDTO());
    featuregroupDTO.setDescriptiveStatistics(statisticsDTO.getDescriptiveStatsDTO());
    featuregroupDTO.setFeaturesHistogram(statisticsDTO.getFeatureDistributionsDTO());
    featuregroupDTO.setFeatureCorrelationMatrix(statisticsDTO.getFeatureCorrelationMatrixDTO());
    featuregroupDTO.setFeaturegroupType(FeaturegroupType.CACHED_FEATURE_GROUP);
    return featuregroupDTO;
  }
  
  public FeaturestoreCreateFeaturegroup setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setVersion(int version) {
    this.version = version;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setCorrMethod(String corrMethod) {
    this.corrMethod = corrMethod;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setNumBins(int numBins) {
    this.numBins = numBins;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setNumClusters(int numClusters) {
    this.numClusters = numClusters;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setMode(String mode) {
    this.mode = mode;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setDataframe(Dataset<Row> dataframe) {
    this.dataframe = dataframe;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setDescriptiveStats(Boolean descriptiveStats) {
    this.descriptiveStats = descriptiveStats;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setFeatureCorr(Boolean featureCorr) {
    this.featureCorr = featureCorr;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setFeatureHistograms(Boolean featureHistograms) {
    this.featureHistograms = featureHistograms;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setClusterAnalysis(Boolean clusterAnalysis) {
    this.clusterAnalysis = clusterAnalysis;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setStatColumns(List<String> statColumns) {
    this.statColumns = statColumns;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setJobs(List<String> jobs) {
    this.jobs = jobs;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setPrimaryKey(String primaryKey) {
    this.primaryKey = primaryKey;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setDescription(String description) {
    this.description = description;
    return this;
  }
  
  public FeaturestoreCreateFeaturegroup setPartitionBy(List<String> partitionBy) {
    this.partitionBy = partitionBy;
    return this;
  }

  public FeaturestoreCreateFeaturegroup setSqlQuery(String sqlQuery) {
    this.sqlQuery = sqlQuery;
    return this;
  }

  public FeaturestoreCreateFeaturegroup setJdbcConnector(String jdbcConnector) {
    this.jdbcConnector = jdbcConnector;
    return this;
  }

  public FeaturestoreCreateFeaturegroup setOnDemand(Boolean onDemand) {
    this.onDemand = onDemand;
    return this;
  }

  public FeaturestoreCreateFeaturegroup setHudi(boolean hudi) {
    this.hudi = hudi;
    return this;
  }

  public FeaturestoreCreateFeaturegroup setHudiArgs(Map<String, String> hudiArgs) {
    this.hudiArgs = hudiArgs;
    return this;
  }

  public FeaturestoreCreateFeaturegroup setHudiTableBasePath(String hudiTableBasePath) {
    this.hudiTableBasePath = hudiTableBasePath;
    return this;
  }
  
}
