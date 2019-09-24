package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.Hops;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturegroupCreationError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.exceptions.InvalidPrimaryKeyForFeaturegroup;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.OnlineFeaturestorePasswordNotFound;
import io.hops.util.exceptions.OnlineFeaturestoreUserNotFound;
import io.hops.util.exceptions.StorageConnectorDoesNotExistError;
import io.hops.util.exceptions.StorageConnectorTypeNotSupportedForFeatureImport;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetFormatNotSupportedError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreS3ConnectorDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreStorageConnectorDTO;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetType;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import org.apache.parquet.Strings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;

/**
 * Builder class for Import-Featuregroup operation on the Hopsworks Featurestore
 */
public class FeaturestoreImportFeaturegroup extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the featuregroup to create
   */
  public FeaturestoreImportFeaturegroup(String name) {
    super(name);
  }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }

  /**
   * Imports an external dataset into the Feature Store as a new Feature group
   *
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException JAXBException JAXBException
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws StorageConnectorTypeNotSupportedForFeatureImport StorageConnectorTypeNotSupportedForFeatureImport
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws IOException IOException
   * @throws InvalidPrimaryKeyForFeaturegroup InvalidPrimaryKeyForFeaturegroup
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws FeaturegroupCreationError FeaturegroupCreationError
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   */
  public void write() throws FeaturestoreNotFound, JAXBException, StorageConnectorDoesNotExistError,
    StorageConnectorTypeNotSupportedForFeatureImport, TrainingDatasetFormatNotSupportedError, HiveNotEnabled,
    TrainingDatasetDoesNotExistError, IOException, InvalidPrimaryKeyForFeaturegroup, DataframeIsEmpty,
    FeaturegroupCreationError, JWTNotFoundException, OnlineFeaturestoreUserNotFound,
    OnlineFeaturestorePasswordNotFound {
    // Get metadata cache
    FeaturestoreMetadataDTO featurestoreMetadataDTO =
      Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read();
    //Get Spark Session
    SparkSession sparkSession = FeaturestoreHelper.sparkGetOrDefault(spark);
    // Read external dataset
    Dataset<Row> sparkDf = doReadExternalFeaturegroup(featurestoreMetadataDTO, sparkSession);
    // Save external dataset as a new Feature Group
    new FeaturestoreCreateFeaturegroup(name).setFeaturestore(featurestore).setSpark(sparkSession).setVersion(version)
      .setCorrMethod(corrMethod).setNumBins(numBins).setNumClusters(numClusters).setMode(mode).setDataframe(sparkDf)
      .setDescriptiveStats(descriptiveStats).setFeatureCorr(featureCorr).setFeatureHistograms(featureHistograms)
      .setClusterAnalysis(clusterAnalysis).setStatColumns(statColumns).setJobs(jobs).setDescription(description)
      .setPartitionBy(partitionBy).write();
  }
  
  /**
   * Reads a dataset from some external storage (currently only S3 is supported)
   *
   * @param featurestoreMetadataDTO metadata of the feature store
   * @param sparkSession the spark session
   * @return a spark dataframe with the data of the external dataset
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws StorageConnectorTypeNotSupportedForFeatureImport StorageConnectorTypeNotSupportedForFeatureImport
   * @throws IOException IOException
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   */
  private Dataset<Row> doReadExternalFeaturegroup(FeaturestoreMetadataDTO featurestoreMetadataDTO,
    SparkSession sparkSession)
    throws StorageConnectorDoesNotExistError, StorageConnectorTypeNotSupportedForFeatureImport, IOException,
    TrainingDatasetDoesNotExistError, TrainingDatasetFormatNotSupportedError {
    FeaturestoreStorageConnectorDTO connectorDTO = FeaturestoreHelper.findStorageConnector(
      featurestoreMetadataDTO.getStorageConnectors(), storageConnector);
    if(!featurestoreMetadataDTO.getSettings()
      .getFeatureImportConnectors().contains(connectorDTO.getStorageConnectorType().name())) {
      throw new StorageConnectorTypeNotSupportedForFeatureImport("The provided storage connector type for " +
        "importing the" +
        " feature data is no in the list of supported connector types for feature import. Provided connector type: "
        + connectorDTO.getStorageConnectorType().name() + ", supported connector types: " +
        Strings.join(featurestoreMetadataDTO.getSettings().getFeatureImportConnectors(), ","));
    }
    switch (connectorDTO.getStorageConnectorType()) {
      case S3:
        return doReadFeaturegroupFromS3(((FeaturestoreS3ConnectorDTO) connectorDTO), sparkSession);
      default:
        throw new StorageConnectorTypeNotSupportedForFeatureImport("The provided storage connector type for " +
          "importing the" +
          " feature data is no in the list of supported connector types for feature import. Provided connector type: "
          + connectorDTO.getStorageConnectorType().name() + ", supported connector types: " +
          Strings.join(featurestoreMetadataDTO.getSettings().getFeatureImportConnectors(), ","));
    }
  }
  
  /**
   * Reads a dataset stored in S3 using Spark and a storage connector configured for the Feature Store
   *
   * @param s3ConnectorDTO the s3 connector
   * @param sparkSession the spark session
   * @return a spark dataframe with the data of the external dataset
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws IOException IOException
   */
  private Dataset<Row> doReadFeaturegroupFromS3(FeaturestoreS3ConnectorDTO s3ConnectorDTO, SparkSession sparkSession)
    throws TrainingDatasetFormatNotSupportedError, TrainingDatasetDoesNotExistError, IOException {
    String path = FeaturestoreHelper.getBucketPath(s3ConnectorDTO.getBucket(), externalPath);
    FeaturestoreHelper.setupS3CredentialsForSpark(s3ConnectorDTO.getAccessKey(), s3ConnectorDTO.getSecretKey(),
      sparkSession);
    return FeaturestoreHelper.getTrainingDataset(sparkSession, dataFormat, path,
      TrainingDatasetType.EXTERNAL_TRAINING_DATASET);
  }
  
  public FeaturestoreImportFeaturegroup setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setVersion(int version) {
    this.version = version;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setCorrMethod(String corrMethod) {
    this.corrMethod = corrMethod;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setNumBins(int numBins) {
    this.numBins = numBins;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setNumClusters(int numClusters) {
    this.numClusters = numClusters;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setMode(String mode) {
    this.mode = mode;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setDataframe(Dataset<Row> dataframe) {
    this.dataframe = dataframe;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setDescriptiveStats(Boolean descriptiveStats) {
    this.descriptiveStats = descriptiveStats;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setFeatureCorr(Boolean featureCorr) {
    this.featureCorr = featureCorr;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setFeatureHistograms(Boolean featureHistograms) {
    this.featureHistograms = featureHistograms;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setClusterAnalysis(Boolean clusterAnalysis) {
    this.clusterAnalysis = clusterAnalysis;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setStatColumns(List<String> statColumns) {
    this.statColumns = statColumns;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setJobs(List<String> jobs) {
    this.jobs = jobs;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setPrimaryKey(String primaryKey) {
    this.primaryKey = primaryKey;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setDescription(String description) {
    this.description = description;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setPartitionBy(List<String> partitionBy) {
    this.partitionBy = partitionBy;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setStorageConnector(String storageConnector) {
    this.storageConnector = storageConnector;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setExternalPath(String externalPath) {
    this.externalPath = externalPath;
    return this;
  }
  
  public FeaturestoreImportFeaturegroup setDataFormat(String dataFormat) {
    this.dataFormat = dataFormat;
    return this;
  }
}
