/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hops.util.featurestore;


import com.google.common.base.Strings;
import io.hops.util.Constants;
import io.hops.util.FeaturestoreRestClient;
import io.hops.util.Hops;
import io.hops.util.exceptions.CannotWriteImageDataFrameException;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.exceptions.InferTFRecordSchemaError;
import io.hops.util.exceptions.InvalidPrimaryKeyForFeaturegroup;
import io.hops.util.exceptions.OnlineFeaturestoreNotEnabled;
import io.hops.util.exceptions.OnlineFeaturestorePasswordNotFound;
import io.hops.util.exceptions.OnlineFeaturestoreUserNotFound;
import io.hops.util.exceptions.SparkDataTypeNotRecognizedError;
import io.hops.util.exceptions.StorageConnectorDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetFormatNotSupportedError;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.app.SQLJoinDTO;
import io.hops.util.featurestore.dtos.feature.FeatureDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupType;
import io.hops.util.featurestore.dtos.featuregroup.OnDemandFeaturegroupDTO;
import io.hops.util.featurestore.dtos.settings.FeaturestoreClientSettingsDTO;
import io.hops.util.featurestore.dtos.stats.StatisticsDTO;
import io.hops.util.featurestore.dtos.stats.cluster_analysis.ClusterAnalysisDTO;
import io.hops.util.featurestore.dtos.stats.cluster_analysis.ClusterDTO;
import io.hops.util.featurestore.dtos.stats.cluster_analysis.DatapointDTO;
import io.hops.util.featurestore.dtos.stats.desc_stats.DescriptiveStatsDTO;
import io.hops.util.featurestore.dtos.stats.desc_stats.DescriptiveStatsMetricValueDTO;
import io.hops.util.featurestore.dtos.stats.desc_stats.DescriptiveStatsMetricValuesDTO;
import io.hops.util.featurestore.dtos.stats.feature_correlation.CorrelationValueDTO;
import io.hops.util.featurestore.dtos.stats.feature_correlation.FeatureCorrelationDTO;
import io.hops.util.featurestore.dtos.stats.feature_correlation.FeatureCorrelationMatrixDTO;
import io.hops.util.featurestore.dtos.stats.feature_distributions.FeatureDistributionDTO;
import io.hops.util.featurestore.dtos.stats.feature_distributions.FeatureDistributionsDTO;
import io.hops.util.featurestore.dtos.stats.feature_distributions.HistogramBinDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreJdbcConnectorDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreStorageConnectorDTO;
import io.hops.util.featurestore.dtos.trainingdataset.HopsfsTrainingDatasetDTO;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetDTO;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetType;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadFeaturegroup;
import io.hops.util.featurestore.ops.read_ops.FeaturestoreReadMetadata;
import io.hops.util.featurestore.ops.write_ops.FeaturestoreUpdateMetadataCache;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.NumericType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

/**
 * Class with helper methods for the featurestore API, used by Hops.java
 */
public class FeaturestoreHelper {

  //Class is supposed to be used from static context only
  private FeaturestoreHelper() {
  }

  /**
   * JAXB config for unmarshalling and marshalling responses/requests from/to Hopsworks REST API
   */
  private static final Logger LOG = Logger.getLogger(FeaturestoreHelper.class.getName());
  private static JAXBContext featuregroupJAXBContext;
  private static JAXBContext descriptiveStatsJAXBContext;
  private static JAXBContext featureCorrelationJAXBContext;
  private static JAXBContext featureHistogramsJAXBContext;
  private static JAXBContext clusterAnalysisJAXBContext;
  private static JAXBContext featureJAXBContext;
  private static JAXBContext featurestoreMetadataJAXBContext;
  private static JAXBContext trainingDatasetJAXBContext;
  private static JAXBContext jdbcConnectorJAXBContext;

  private static Marshaller descriptiveStatsMarshaller;
  private static Marshaller featureCorrelationMarshaller;
  private static Marshaller featureHistogramsMarshaller;
  private static Marshaller clusteranalysisMarshaller;
  private static Marshaller featureMarshaller;
  private static Marshaller featurestoreMetadataMarshaller;
  private static Marshaller trainingDatasetMarshaller;
  private static Marshaller featuregroupMarshaller;
  private static Marshaller jdbcConnectorMarshaller;
  
  

  /**
   * Featurestore Metadata Cache
   */
  private static FeaturestoreMetadataDTO featurestoreMetadataCache = null;

  static {
    try {
      descriptiveStatsJAXBContext =
          JAXBContextFactory.createContext(new Class[]{DescriptiveStatsDTO.class}, null);
      featureCorrelationJAXBContext =
          JAXBContextFactory.createContext(new Class[]{FeatureCorrelationMatrixDTO.class}, null);
      featureHistogramsJAXBContext =
          JAXBContextFactory.createContext(new Class[]{FeatureDistributionsDTO.class}, null);
      clusterAnalysisJAXBContext =
          JAXBContextFactory.createContext(new Class[]{ClusterAnalysisDTO.class}, null);
      featureJAXBContext =
          JAXBContextFactory.createContext(new Class[]{FeatureDTO.class}, null);
      featurestoreMetadataJAXBContext =
          JAXBContextFactory.createContext(new Class[]{FeaturestoreMetadataDTO.class}, null);
      trainingDatasetJAXBContext =
          JAXBContextFactory.createContext(new Class[]{TrainingDatasetDTO.class}, null);
      featuregroupJAXBContext =
        JAXBContextFactory.createContext(new Class[]{FeaturegroupDTO.class}, null);
      jdbcConnectorJAXBContext =
        JAXBContextFactory.createContext(new Class[]{FeaturestoreJdbcConnectorDTO.class}, null);
      descriptiveStatsMarshaller = descriptiveStatsJAXBContext.createMarshaller();
      descriptiveStatsMarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      descriptiveStatsMarshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      featureCorrelationMarshaller = featureCorrelationJAXBContext.createMarshaller();
      featureCorrelationMarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      featureCorrelationMarshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      clusteranalysisMarshaller = clusterAnalysisJAXBContext.createMarshaller();
      clusteranalysisMarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      clusteranalysisMarshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      featureHistogramsMarshaller = featureHistogramsJAXBContext.createMarshaller();
      featureHistogramsMarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      featureHistogramsMarshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      featureMarshaller = featureJAXBContext.createMarshaller();
      featureMarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      featureMarshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      featurestoreMetadataMarshaller = featurestoreMetadataJAXBContext.createMarshaller();
      featurestoreMetadataMarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      featurestoreMetadataMarshaller.setProperty(
          MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      trainingDatasetMarshaller = trainingDatasetJAXBContext.createMarshaller();
      trainingDatasetMarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      trainingDatasetMarshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      featuregroupMarshaller = featuregroupJAXBContext.createMarshaller();
      featuregroupMarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      featuregroupMarshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      jdbcConnectorMarshaller = jdbcConnectorJAXBContext.createMarshaller();
      jdbcConnectorMarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      jdbcConnectorMarshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
    } catch (JAXBException e) {
      LOG.log(Level.SEVERE, "An error occurred while initializing JAXBContext", e);
    }
  }

  /**
   * Gets the project's featurestore name (project_featurestore)
   *
   * @return the featurestore name (hive db)
   */
  public static String getProjectFeaturestore() {
    String projectName = Hops.getProjectName();
    return projectName.toLowerCase() + Constants.FEATURESTORE_SUFFIX;
  }

  /**
   * Selects the featurestore database in SparkSQL
   *
   * @param sparkSession the spark session
   * @param featurestore the featurestore database to select
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  public static void useFeaturestore(SparkSession sparkSession, String featurestore)
    throws OnlineFeaturestorePasswordNotFound, FeaturestoreNotFound, OnlineFeaturestoreUserNotFound, JAXBException,
    OnlineFeaturestoreNotEnabled {
    if (featurestore == null)
      featurestore = getProjectFeaturestore();
    String sqlStr = "use " + featurestore;
    logAndRunSQL(sparkSession, sqlStr, false, featurestore);
  }

  /**
   * Helper function that gets the Hive table name for a featuregroup with a particular version
   *
   * @param featuregroupName name of the featuregroup
   * @param version          version of the featuregroup
   * @return hive table name
   */
  public static String getTableName(String featuregroupName, int version) {
    return featuregroupName + "_" + version;
  }
  
  
  /**
   * Saves the given dataframe to online featuregroup.
   *
   * @param sparkDf             the dataframe containing the data to insert into the featuregroup
   * @param featuregroup        the name of the featuregroup (hive table name)
   * @param featurestore        the featurestore to save the featuregroup to (hive database)
   * @param featuregroupVersion the version of the featuregroup
   * @param mode                the write mode
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   */
  public static void insertIntoOnlineFeaturegroup(Dataset<Row> sparkDf,
    String featuregroup, String featurestore, int featuregroupVersion, String mode)
    throws OnlineFeaturestorePasswordNotFound, FeaturestoreNotFound, OnlineFeaturestoreUserNotFound, JAXBException {
    FeaturestoreJdbcConnectorDTO featurestoreJdbcConnectorDTO = doGetOnlineFeaturestoreJdbcConnector(featurestore,
      new FeaturestoreReadMetadata().setFeaturestore(featurestore).read());
    String tableName = getTableName(featuregroup, featuregroupVersion);
    writeJdbcDataframe(sparkDf, featurestoreJdbcConnectorDTO, tableName, mode);
  }
  
  /**
   * Saves the given dataframe to the specified featuregroup. Defaults to the project-featurestore
   * This will append to  the featuregroup. To overwrite a featuregroup, create a new version of the featuregroup
   * from the UI and append to that table.
   *
   * @param sparkDf             the dataframe containing the data to insert into the featuregroup
   * @param sparkSession        the spark session
   * @param featuregroup        the name of the featuregroup (hive table name)
   * @param featurestore        the featurestore to save the featuregroup to (hive database)
   * @param featuregroupVersion the version of the featuregroup
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  public static void insertIntoOfflineFeaturegroup(Dataset<Row> sparkDf, SparkSession sparkSession,
                                            String featuregroup, String featurestore,
                                            int featuregroupVersion)
    throws OnlineFeaturestorePasswordNotFound, FeaturestoreNotFound, OnlineFeaturestoreUserNotFound, JAXBException,
    OnlineFeaturestoreNotEnabled {
    useFeaturestore(sparkSession, featurestore);
    String tableName = getTableName(featuregroup, featuregroupVersion);

    SparkContext sc = sparkSession.sparkContext();
    SQLContext sqlContext = new SQLContext(sc);
    sqlContext.setConf("hive.exec.dynamic.partition", "true");
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict");
    //overwrite is not supported because it will drop the table and create a new one,
    //this means that all the featuregroup metadata will be dropped due to ON DELETE CASCADE
    String mode = "append";
    //Specify format hive as it is managed table
    String format = "hive";
    sparkDf.write().format(format).mode(mode).insertInto(tableName);
  }
  
  /**
   *  Writes the given dataframe as a Hudi dataset on HDFS.
   *
   * @param sparkDf the dataframe to write
   * @param sparkSession the spark sesison
   * @param featuregroup the name of the feature group (hive table name)
   * @param featurestore the featurestore to save the featuregroup to (hive database)
   * @param featuregroupVersion the version of the featuregroup
   * @param hudiArgs hudiWriteArguments
   * @param hudiBasePath path for the external table
   * @param mode write mode
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  public static void writeHudiDataset(Dataset<Row> sparkDf, SparkSession sparkSession,
    String featuregroup, String featurestore, int featuregroupVersion, Map<String, String> hudiArgs,
    String hudiBasePath, String mode)
    throws OnlineFeaturestorePasswordNotFound, FeaturestoreNotFound, OnlineFeaturestoreUserNotFound, JAXBException,
    OnlineFeaturestoreNotEnabled {
    useFeaturestore(sparkSession, featurestore);
    DataFrameWriter writer = sparkDf.write().format(Constants.HUDI_SPARK_FORMAT);
    for(Map.Entry<String, String> entry : hudiArgs.entrySet()){
      writer = writer.option(entry.getKey(), entry.getValue());
    }
    writer = writer.mode(mode);
    if(!Strings.isNullOrEmpty(hudiBasePath)) {
      writer.save(hudiBasePath);
    } else {
      writer.save(getDefaultHudiBasePath(featuregroup, featuregroupVersion));
    }
  }
  
  /**
   * Default Hudi base path (external table) for a feature group
   *
   * @param tableName name of the feature group
   * @param version version of the feature group
   * @return default base path for hudi feature group (in resources dataset)
   */
  public static String getDefaultHudiBasePath(String tableName, int version) {
    return ("hdfs:///" + Constants.PROJECT_ROOT_DIR + Constants.SLASH_DELIMITER +
      Hops.getProjectName() + Constants.SLASH_DELIMITER + "Resources" +
      Constants.SLASH_DELIMITER + getTableName(tableName, version));
  }
  
  /**
   * Setup Hive arguments for Hudi Sync Tool to connect to Feature Store Hive Database
   *
   * @param hudiArgs the hudi arguments
   * @param tableName name of the table to sync in Hive
   * @return the updated hudi arguments
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   */
  public static Map<String, String> setupHudiHiveArgs(Map<String, String> hudiArgs, String tableName)
    throws StorageConnectorDoesNotExistError {
    hudiArgs.put(Constants.HUDI_HIVE_SYNC_ENABLE, "true");
    hudiArgs.put(Constants.HUDI_HIVE_SYNC_TABLE, tableName);
    FeaturestoreMetadataDTO featurestoreMetadata = FeaturestoreHelper.getFeaturestoreMetadataCache();
    // Gets the JDBC Connector for the project's Hive metastore
    FeaturestoreJdbcConnectorDTO jdbcConnector = (FeaturestoreJdbcConnectorDTO) FeaturestoreHelper.findStorageConnector(
      featurestoreMetadata.getStorageConnectors(), Hops.getProjectFeaturestore().read());
    hudiArgs.put(Constants.HUDI_HIVE_SYNC_JDBC_URL, getJDBCUrlFromConnector(jdbcConnector,
      new HashMap<String, String>()));
    hudiArgs.put(Constants.HUDI_HIVE_SYNC_DB, Hops.getProjectFeaturestore().read());
    return hudiArgs;
  }
  
  /**
   * Gets the JDBC connection string from a JDBC connector in the feature store. Substitutes arguments if necessary.
   *
   * @param jdbcConnector the jdbc connector to get the string for
   * @param jdbcArguments the arguments for the jdbc connection (e.g password and certificate locations)
   * @return the JDBC string
   */
  public static String getJDBCUrlFromConnector(FeaturestoreJdbcConnectorDTO jdbcConnector,
    Map<String, String> jdbcArguments) {
    String jdbcConnectionString = jdbcConnector.getConnectionString();
    String[] jdbcConnectionStringArguments = jdbcConnector.getArguments().split(",");
  
    // Substitute jdbc arguments in the connection string
    for (int i = 0; i < jdbcConnectionStringArguments.length; i++) {
      if (jdbcArguments != null && jdbcArguments.containsKey(jdbcConnectionStringArguments[i])) {
        jdbcConnectionString = jdbcConnectionString + jdbcConnectionStringArguments[i] +
          Constants.JDBC_CONNECTION_STRING_VALUE_DELIMITER + jdbcArguments.get(jdbcConnectionStringArguments[i]) +
          Constants.JDBC_CONNECTION_STRING_DELIMITER;
      } else {
        if(jdbcConnectionStringArguments[i].equalsIgnoreCase(Constants.JDBC_TRUSTSTORE_ARG)){
          String trustStore = Hops.getTrustStore();
          jdbcConnectionString = jdbcConnectionString + Constants.JDBC_TRUSTSTORE_ARG +
            Constants.JDBC_CONNECTION_STRING_VALUE_DELIMITER + trustStore +
            Constants.JDBC_CONNECTION_STRING_DELIMITER;
        }
        if(jdbcConnectionStringArguments[i].equalsIgnoreCase(Constants.JDBC_TRUSTSTORE_PW_ARG)){
          String pw = Hops.getKeystorePwd();
          jdbcConnectionString = jdbcConnectionString + Constants.JDBC_TRUSTSTORE_PW_ARG +
            Constants.JDBC_CONNECTION_STRING_VALUE_DELIMITER + pw +
            Constants.JDBC_CONNECTION_STRING_DELIMITER;
        }
        if(jdbcConnectionStringArguments[i].equalsIgnoreCase(Constants.JDBC_KEYSTORE_ARG)){
          String keyStore = Hops.getKeyStore();
          jdbcConnectionString = jdbcConnectionString + Constants.JDBC_KEYSTORE_ARG +
            Constants.JDBC_CONNECTION_STRING_VALUE_DELIMITER + keyStore +
            Constants.JDBC_CONNECTION_STRING_DELIMITER;
        }
        if(jdbcConnectionStringArguments[i].equalsIgnoreCase(Constants.JDBC_KEYSTORE_PW_ARG)){
          String pw = Hops.getKeystorePwd();
          jdbcConnectionString = jdbcConnectionString + Constants.JDBC_KEYSTORE_PW_ARG +
            Constants.JDBC_CONNECTION_STRING_VALUE_DELIMITER + pw +
            Constants.JDBC_CONNECTION_STRING_DELIMITER;
        }
      }
    }
    return jdbcConnectionString;
  }

  /**
   * Go through list of featuregroups and find the ones that contain the feature
   *
   * @param featuregroups featuregroups to search through
   * @param feature       the feature to look for
   * @return a list of featuregroup names and versions for featuregroups that contain the given feature
   */
  private static List<FeaturegroupDTO> findFeaturegroupThatContainsFeature(
      List<FeaturegroupDTO> featuregroups, String feature) {
    List<FeaturegroupDTO> matches = new ArrayList<>();
    for (FeaturegroupDTO featuregroupDTO : featuregroups) {
      for (FeatureDTO featureDTO : featuregroupDTO.getFeatures()) {
        String fullName = getTableName(featuregroupDTO.getName(), featuregroupDTO.getVersion()) + "." +
            featureDTO.getName();
        if (featureDTO.getName().equals(feature) || fullName.equals(feature)) {
          matches.add(featuregroupDTO);
          break;
        }
      }
    }
    return matches;
  }

  /**
   * Tries to find a list of featuregroups that contain a requested set of features by searching through
   * data from the metastore
   *
   * @param featuregroups the list of all featuregroups from the metastore
   * @param features      the list of features requested
   * @param featurestore  the featurestore to search through
   * @return a list of featuregroups such that each feature is included in exactly one featuregroup
   */
  public static List<FeaturegroupDTO> findFeaturegroupsThatContainsFeatures(
      List<FeaturegroupDTO> featuregroups,
      List<String> features, String featurestore) {
    Set<FeaturegroupDTO> featureFeaturegroupsSet = new HashSet<>();
    for (String feature : features) {
      FeaturegroupDTO featuregroupDTO = findFeature(feature, featurestore, featuregroups);
      featureFeaturegroupsSet.add(featuregroupDTO);
    }
    List<FeaturegroupDTO> featureFeaturegroups = new ArrayList<>();
    featureFeaturegroups.addAll(featureFeaturegroupsSet);
    return featureFeaturegroups;
  }

  /**
   * Gets a cached featuregroup from a particular featurestore
   *
   * @param sparkSession        the spark session
   * @param featuregroup        the featuregroup to get
   * @param featurestore        the featurestore to query
   * @param featuregroupVersion the version of the featuregroup to get
   * @param online              boolean flag whether to read from the online feature store
   * @return a spark dataframe with the featuregroup
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   */
  public static Dataset<Row> getCachedFeaturegroup(SparkSession sparkSession, String featuregroup,
                                                   String featurestore, int featuregroupVersion, Boolean online)
    throws OnlineFeaturestorePasswordNotFound, FeaturestoreNotFound, OnlineFeaturestoreUserNotFound, JAXBException,
    OnlineFeaturestoreNotEnabled, FeaturegroupDoesNotExistError {
    useFeaturestore(sparkSession, featurestore);
    String sqlStr = "SELECT * FROM " + getTableName(featuregroup, featuregroupVersion);
    Dataset<Row> sparkDf = logAndRunSQL(sparkSession, sqlStr, online, featurestore);
    List<FeaturegroupDTO> featuregroups = new ArrayList<>();
    FeaturestoreMetadataDTO featurestoreMetadataDTO = getFeaturestoreMetadataCache();
    FeaturegroupDTO featuregroupDTO =
      findFeaturegroup(featurestoreMetadataDTO.getFeaturegroups(), featuregroup, featuregroupVersion);
    featuregroups.add(featuregroupDTO);
    List<String> features = featuregroupDTO.getFeatures().stream().map(f -> f.getName()).collect(Collectors.toList());
    Map<String, String> featureToFeaturegroupMapping =
      getFeatureToFeaturegroupMapping(featuregroups, features, featurestore);
    sparkSession.sparkContext().setJobGroup("", "", true);
    return addProvenanceMetadataToDataFrame(sparkDf, featureToFeaturegroupMapping);
  }

  /**
   * Gets a on-demand feature group as a Spark dataframe
   *
   * @param sparkSession the spark session
   * @param featurestore the featurestore
   * @param onDemandFeaturegroupDTO DTO of the feature group
   * @param connectionString the JDBC connection string
   * @return on demand feature group spark dataframe
   */
  public static Dataset<Row> getOnDemandFeaturegroup(SparkSession sparkSession,
    OnDemandFeaturegroupDTO onDemandFeaturegroupDTO, String connectionString, String featurestore) {
    //Read on-demand feature group using JDBC
    Dataset<Row> sparkDf = sparkSession.read().format(Constants.SPARK_JDBC_FORMAT)
        .option(Constants.SPARK_JDBC_URL, connectionString)
        .option(Constants.SPARK_JDBC_DBTABLE, "(" + onDemandFeaturegroupDTO.getQuery() + ") fs_q")
        .load();
    //Remove Column Prefixes
    List<String> schemaNames = Arrays.asList(sparkDf.schema().fieldNames())
        .stream().map(name -> name.replace("fs_q.", "")).collect(Collectors.toList());
    Dataset<Row> renamedSparkDf = sparkDf.toDF(convertListToSeq(schemaNames));
    sparkSession.sparkContext().setJobGroup("", "", true);
    List<FeaturegroupDTO> featuregroups = new ArrayList<>();
    featuregroups.add(onDemandFeaturegroupDTO);
    List<String> features =
      onDemandFeaturegroupDTO.getFeatures().stream().map(f -> f.getName()).collect(Collectors.toList());
    Map<String, String> featureToFeaturegroupMapping =
      getFeatureToFeaturegroupMapping(featuregroups, features, featurestore);
    return addProvenanceMetadataToDataFrame(renamedSparkDf, featureToFeaturegroupMapping);
  }


  /**
   * Gets the partitions of a featuregroup from a particular featurestore
   *
   * @param sparkSession        the spark session
   * @param featuregroup        the featuregroup to get partitions for
   * @param featurestore        the featurestore where the featuregroup resides
   * @param featuregroupVersion the version of the featuregroup
   * @param online                whether to read from the online feature group or offline
   * @return a spark dataframe with the partitions of the featuregroup
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  public static Dataset<Row> getFeaturegroupPartitions(SparkSession sparkSession, String featuregroup,
    String featurestore, int featuregroupVersion, Boolean online)
    throws OnlineFeaturestorePasswordNotFound, FeaturestoreNotFound, OnlineFeaturestoreUserNotFound, JAXBException,
    OnlineFeaturestoreNotEnabled {
    useFeaturestore(sparkSession, featurestore);
    String sqlStr = "SHOW PARTITIONS " + getTableName(featuregroup, featuregroupVersion);
    return logAndRunSQL(sparkSession, sqlStr, online, featurestore);
  }

  /**
   * Gets a training dataset from a feature store
   *
   * @param sparkSession the spark session
   * @param dataFormat   the data format of the training dataset
   * @param path     the path to the dataset (hdfs or s3 path)
   * @param trainingDatasetType the type of the training dataset
   * @return a spark dataframe with the dataset
   * @throws TrainingDatasetFormatNotSupportedError if a unsupported data format is provided, supported modes are:
   *                                                tfrecords, tsv, csv, avro, orc, image and parquet
   * @throws IOException IOException IOException
   * @throws TrainingDatasetDoesNotExistError if the path is not found
   */
  public static Dataset<Row> getTrainingDataset(SparkSession sparkSession, String dataFormat, String path,
    TrainingDatasetType trainingDatasetType)
      throws TrainingDatasetFormatNotSupportedError, IOException, TrainingDatasetDoesNotExistError {
    Configuration hdfsConf = new Configuration();
    Path filePath = null;
    FileSystem hdfs = null;
    switch (dataFormat) {
      case Constants.TRAINING_DATASET_CSV_FORMAT:
        if(trainingDatasetType == TrainingDatasetType.HOPSFS_TRAINING_DATASET) {
          filePath = new org.apache.hadoop.fs.Path(path);
          hdfs = filePath.getFileSystem(hdfsConf);
          if (hdfs.exists(filePath)) {
            return sparkSession.read().format(dataFormat).option(Constants.SPARK_WRITE_HEADER, "true")
              .option(Constants.SPARK_WRITE_DELIMITER, Constants.COMMA_DELIMITER).load(path);
          } else {
            filePath = new org.apache.hadoop.fs.Path(path + Constants.TRAINING_DATASET_CSV_SUFFIX);
            hdfs = filePath.getFileSystem(hdfsConf);
            if (hdfs.exists(filePath)) {
              return sparkSession.read().format(dataFormat).option(Constants.SPARK_WRITE_HEADER, "true")
                .option(Constants.SPARK_WRITE_DELIMITER, Constants.COMMA_DELIMITER).load(path +
                  Constants.TRAINING_DATASET_CSV_SUFFIX);
            } else {
              throw new TrainingDatasetDoesNotExistError("Could not find any training dataset in folder : "
                + path + " or in file: " + path + Constants.TRAINING_DATASET_CSV_SUFFIX);
            }
          }
        } else {
          return sparkSession.read().format(dataFormat).option(Constants.SPARK_WRITE_HEADER, "true")
            .option(Constants.SPARK_WRITE_DELIMITER, Constants.COMMA_DELIMITER).load(path);
        }
      case Constants.TRAINING_DATASET_TSV_FORMAT:
        if(trainingDatasetType == TrainingDatasetType.HOPSFS_TRAINING_DATASET) {
          filePath = new org.apache.hadoop.fs.Path(path);
          hdfs = filePath.getFileSystem(hdfsConf);
          if (hdfs.exists(filePath)) {
            return sparkSession.read().format(dataFormat).option(Constants.SPARK_WRITE_HEADER, "true")
              .option(Constants.SPARK_WRITE_DELIMITER, Constants.TAB_DELIMITER).load(path);
          } else {
            filePath = new org.apache.hadoop.fs.Path(path + Constants.TRAINING_DATASET_TSV_SUFFIX);
            hdfs = filePath.getFileSystem(hdfsConf);
            if (hdfs.exists(filePath)) {
              return sparkSession.read().format(dataFormat).option(Constants.SPARK_WRITE_HEADER, "true")
                .option(Constants.SPARK_WRITE_DELIMITER, Constants.TAB_DELIMITER).load(path +
                  Constants.TRAINING_DATASET_TSV_SUFFIX);
            } else {
              throw new TrainingDatasetDoesNotExistError("Could not find any training dataset in folder : "
                + path + " or in file: " + path + Constants.TRAINING_DATASET_TSV_SUFFIX);
            }
          }
        } else {
          return sparkSession.read().format(dataFormat).option(Constants.SPARK_WRITE_HEADER, "true")
            .option(Constants.SPARK_WRITE_DELIMITER, Constants.TAB_DELIMITER).load(path);
        }
      case Constants.TRAINING_DATASET_PARQUET_FORMAT:
        if(trainingDatasetType == TrainingDatasetType.HOPSFS_TRAINING_DATASET) {
          filePath = new org.apache.hadoop.fs.Path(path);
          hdfs = filePath.getFileSystem(hdfsConf);
          if (hdfs.exists(filePath)) {
            return sparkSession.read().parquet(path);
          } else {
            filePath = new org.apache.hadoop.fs.Path(path + Constants.TRAINING_DATASET_PARQUET_SUFFIX);
            hdfs = filePath.getFileSystem(hdfsConf);
            if (hdfs.exists(filePath)) {
              return sparkSession.read().parquet(path + Constants.TRAINING_DATASET_PARQUET_SUFFIX);
            } else {
              throw new TrainingDatasetDoesNotExistError("Could not find any training dataset in folder : "
                + path + " or in file: " + path + Constants.TRAINING_DATASET_PARQUET_SUFFIX);
            }
          }
        } else {
          return sparkSession.read().parquet(path);
        }
      case Constants.TRAINING_DATASET_AVRO_FORMAT:
        if(trainingDatasetType == TrainingDatasetType.HOPSFS_TRAINING_DATASET) {
          filePath = new org.apache.hadoop.fs.Path(path);
          hdfs = filePath.getFileSystem(hdfsConf);
          if (hdfs.exists(filePath)) {
            return sparkSession.read().format(dataFormat).load(path);
          } else {
            filePath = new org.apache.hadoop.fs.Path(path + Constants.TRAINING_DATASET_AVRO_SUFFIX);
            hdfs = filePath.getFileSystem(hdfsConf);
            if (hdfs.exists(filePath)) {
              return sparkSession.read().format(dataFormat).load(path + Constants.TRAINING_DATASET_AVRO_SUFFIX);
            } else {
              throw new TrainingDatasetDoesNotExistError("Could not find any training dataset in folder : "
                + path + " or in file: " + path + Constants.TRAINING_DATASET_AVRO_SUFFIX);
            }
          }
        } else {
          return sparkSession.read().format(dataFormat).load(path);
        }
      case Constants.TRAINING_DATASET_ORC_FORMAT:
        if(trainingDatasetType == TrainingDatasetType.HOPSFS_TRAINING_DATASET) {
          filePath = new org.apache.hadoop.fs.Path(path);
          hdfs = filePath.getFileSystem(hdfsConf);
          if (hdfs.exists(filePath)) {
            return sparkSession.read().format(dataFormat).load(path);
          } else {
            filePath = new org.apache.hadoop.fs.Path(path + Constants.TRAINING_DATASET_ORC_SUFFIX);
            hdfs = filePath.getFileSystem(hdfsConf);
            if (hdfs.exists(filePath)) {
              return sparkSession.read().format(dataFormat).load(path + Constants.TRAINING_DATASET_ORC_SUFFIX);
            } else {
              throw new TrainingDatasetDoesNotExistError("Could not find any training dataset in folder : "
                + path + " or in file: " + path + Constants.TRAINING_DATASET_ORC_SUFFIX);
            }
          }
        } else {
          return sparkSession.read().format(dataFormat).load(path);
        }
      case Constants.TRAINING_DATASET_IMAGE_FORMAT:
        if(trainingDatasetType == TrainingDatasetType.HOPSFS_TRAINING_DATASET) {
          filePath = new org.apache.hadoop.fs.Path(path);
          hdfs = filePath.getFileSystem(hdfsConf);
          if (hdfs.exists(filePath)) {
            return sparkSession.read().format(dataFormat).load(path);
          } else {
            filePath = new org.apache.hadoop.fs.Path(path + Constants.TRAINING_DATASET_IMAGE_SUFFIX);
            hdfs = filePath.getFileSystem(hdfsConf);
            if (hdfs.exists(filePath)) {
              return sparkSession.read().format(dataFormat).load(path + Constants.TRAINING_DATASET_IMAGE_SUFFIX);
            } else {
              throw new TrainingDatasetDoesNotExistError("Could not find any training dataset in folder : "
                + path + " or in file: " + path + Constants.TRAINING_DATASET_IMAGE_SUFFIX);
            }
          }
        } else {
          return sparkSession.read().format(dataFormat).load(path);
        }
      case Constants.TRAINING_DATASET_TFRECORDS_FORMAT:
        if(trainingDatasetType == TrainingDatasetType.HOPSFS_TRAINING_DATASET) {
          filePath = new org.apache.hadoop.fs.Path(path);
          hdfs = filePath.getFileSystem(hdfsConf);
          if (hdfs.exists(filePath)) {
            return sparkSession.read().format(dataFormat).option(Constants.SPARK_TF_CONNECTOR_RECORD_TYPE,
              Constants.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(path);
          } else {
            filePath = new org.apache.hadoop.fs.Path(path + Constants.TRAINING_DATASET_TFRECORDS_SUFFIX);
            hdfs = filePath.getFileSystem(hdfsConf);
            if (hdfs.exists(filePath)) {
              return sparkSession.read().format(dataFormat).option(Constants.SPARK_TF_CONNECTOR_RECORD_TYPE,
                Constants.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(path +
                Constants.TRAINING_DATASET_TFRECORDS_SUFFIX);
            } else {
              throw new TrainingDatasetDoesNotExistError("Could not find any training dataset in folder : "
                + path + " or in file: " + path + Constants.TRAINING_DATASET_TFRECORDS_SUFFIX);
            }
          }
        } else {
          return sparkSession.read().format(dataFormat).option(Constants.SPARK_TF_CONNECTOR_RECORD_TYPE,
            Constants.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(path);
        }
      default:
        throw new TrainingDatasetFormatNotSupportedError("The provided data format: " + dataFormat +
            " is not supported in the Java/Scala API, the supported data formats are: " +
            Constants.TRAINING_DATASET_CSV_FORMAT + "," +
            Constants.TRAINING_DATASET_TSV_FORMAT + "," +
            Constants.TRAINING_DATASET_TFRECORDS_FORMAT + "," +
            Constants.TRAINING_DATASET_PARQUET_FORMAT + "," +
            Constants.TRAINING_DATASET_AVRO_FORMAT + "," +
            Constants.TRAINING_DATASET_ORC_FORMAT + "," +
            Constants.TRAINING_DATASET_IMAGE_FORMAT + ","
        );
    }
  }

  /**
   * Searches a list of featuregroups for a particular feature and returns the matching featuregroup
   *
   * @param feature          the feature to search for
   * @param featurestore     the featurestore to query
   * @param featuregroupDTOS the list of featuregroups to search through
   * @return the matching featuregroup
   */
  private static FeaturegroupDTO findFeature(String feature, String featurestore,
                                             List<FeaturegroupDTO> featuregroupDTOS) {
    List<FeaturegroupDTO> matchedFeaturegroups = findFeaturegroupThatContainsFeature(featuregroupDTOS, feature);
    if (matchedFeaturegroups.isEmpty()) {
      throw new IllegalArgumentException("Could not find the feature with name: " + feature +
          " in any of the featuregroups of the featurestore: " + featurestore);
    }
    if (matchedFeaturegroups.size() > 1) {
      List<String> featuregroupStrings = featuregroupDTOS.stream()
          .map(fg -> getTableName(fg.getName(), fg.getVersion())).collect(Collectors.toList());
      String featuregroupsStr = StringUtils.join(featuregroupStrings, ", ");
      throw new IllegalArgumentException("Found the feature with name: " + feature +
          " in more than one of the featuregroups of the featurestore " + featurestore +
          " please specify featuregroup that you want to get the feature from." +
          " The matched featuregroups are: " + featuregroupsStr);
    }
    return matchedFeaturegroups.get(0);
  }

  /**
   * Gets a feature from a featurestore given metadata about which featuregroups exist, it will search through the
   * featuregroups and find where the feature is located
   *
   * @param sparkSession     the spark session
   * @param feature          the feature to get
   * @param featurestore     the featurestore to query
   * @param featuregroupDTOS the list of featuregroups metadata
   * @param jdbcArguments    map of jdbc arguments, in case the feature belongs to an on-demand feature group
   * @param online           whether to read from the online feature group or offline
   * @return A dataframe with the feature
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  public static Dataset<Row> getFeature(
      SparkSession sparkSession, String feature,
      String featurestore, List<FeaturegroupDTO> featuregroupDTOS, Map<String, String> jdbcArguments, Boolean online)
    throws FeaturegroupDoesNotExistError, HiveNotEnabled, StorageConnectorDoesNotExistError,
    OnlineFeaturestorePasswordNotFound, FeaturestoreNotFound, OnlineFeaturestoreUserNotFound, JAXBException,
    OnlineFeaturestoreNotEnabled {
    useFeaturestore(sparkSession, featurestore);
    FeaturegroupDTO matchedFeaturegroup = findFeature(feature, featurestore, featuregroupDTOS);
    if(matchedFeaturegroup.getFeaturegroupType() == FeaturegroupType.ON_DEMAND_FEATURE_GROUP){
      List<FeaturegroupDTO> onDemandFeaturegroups = new ArrayList<>();
      onDemandFeaturegroups.add(matchedFeaturegroup);
      Map<String, Map<String, String>> onDemandFeaturegroupsJdbcArguments = new HashMap<>();
      onDemandFeaturegroupsJdbcArguments.put(
          getTableName(matchedFeaturegroup.getName(), matchedFeaturegroup.getVersion()), jdbcArguments);
      registerOnDemandFeaturegroupsAsTempTables(onDemandFeaturegroups, featurestore,
          onDemandFeaturegroupsJdbcArguments);
    }
    String sqlStr = "SELECT " + feature + " FROM " +
        getTableName(matchedFeaturegroup.getName(), matchedFeaturegroup.getVersion());
    List<FeaturegroupDTO> featuregroups = new ArrayList<>();
    featuregroups.add(matchedFeaturegroup);
    List<String> features = new ArrayList<>();
    features.add(feature);
    Map<String, String> featureToFeaturegroupMapping =
      getFeatureToFeaturegroupMapping(featuregroups, features, featurestore);
    Dataset<Row> result = logAndRunSQL(sparkSession, sqlStr, online, featurestore);
    return addProvenanceMetadataToDataFrame(result, featureToFeaturegroupMapping);
  }

  /**
   * Gets a feature from a featurestore and a specific featuregroup.
   *
   * @param sparkSession        the spark session
   * @param feature             the feature to get
   * @param featurestore        the featurestore to query
   * @param featuregroup        the featuregroup where the feature is located
   * @param featuregroupVersion the version of the featuregroup
   * @param jdbcArguments       map of jdbc arguments, in case the feature belongs to an on-demand feature group
   * @param featuregroupDTOs    list of feature groups
   * @param online               whether to read from the online feature group or offline
   * @return the resulting spark dataframe with the feature
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  public static Dataset<Row> getFeature(SparkSession sparkSession, String feature, String featurestore,
                                        String featuregroup, int featuregroupVersion,
                                        List<FeaturegroupDTO> featuregroupDTOs, Map<String, String> jdbcArguments,
                                        Boolean online)
    throws FeaturegroupDoesNotExistError, HiveNotEnabled, StorageConnectorDoesNotExistError,
    OnlineFeaturestorePasswordNotFound, FeaturestoreNotFound, OnlineFeaturestoreUserNotFound, JAXBException,
    OnlineFeaturestoreNotEnabled {
    FeaturegroupDTO featuregroupDTO = findFeaturegroup(featuregroupDTOs, featuregroup, featuregroupVersion);
    if(featuregroupDTO.getFeaturegroupType() == FeaturegroupType.ON_DEMAND_FEATURE_GROUP){
      List<FeaturegroupDTO> onDemandFeaturegroups = new ArrayList<>();
      onDemandFeaturegroups.add(featuregroupDTO);
      Map<String, Map<String, String>> onDemandFeaturegroupsJdbcArguments = new HashMap<>();
      onDemandFeaturegroupsJdbcArguments.put(
          getTableName(featuregroupDTO.getName(), featuregroupDTO.getVersion()), jdbcArguments);
      registerOnDemandFeaturegroupsAsTempTables(onDemandFeaturegroups, featurestore,
          onDemandFeaturegroupsJdbcArguments);
    }
    useFeaturestore(sparkSession, featurestore);
    String sqlStr = "SELECT " + feature + " FROM " + getTableName(featuregroup, featuregroupVersion);
    List<FeaturegroupDTO> featuregroups = new ArrayList<>();
    featuregroups.add(featuregroupDTO);
    List<String> features = new ArrayList<>();
    features.add(feature);
    Map<String, String> featureToFeaturegroupMapping =
      getFeatureToFeaturegroupMapping(featuregroups, features, featurestore);
    Dataset<Row> result = logAndRunSQL(sparkSession, sqlStr, online, featurestore);
    return addProvenanceMetadataToDataFrame(result, featureToFeaturegroupMapping);
  }

  /**
   * Constructs the SQL JOIN-string using a list of featuregroups and a join key
   *
   * @param featuregroupDTOS the list of featuregroups
   * @param joinKey          the join key
   * @return the join string and a list of aliases mapped to featuregroups
   */
  private static SQLJoinDTO getJoinStr(List<FeaturegroupDTO> featuregroupDTOS, String joinKey) {
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 1; i < featuregroupDTOS.size(); i++) {
      stringBuilder.append("JOIN " + getTableName(featuregroupDTOS.get(i).getName(),
          featuregroupDTOS.get(i).getVersion()));
      stringBuilder.append(" ");
    }
    stringBuilder.append("ON ");
    for (int i = 0; i < featuregroupDTOS.size(); i++) {
      if (i != 0 && i < (featuregroupDTOS.size() - 1)) {
        stringBuilder.append(getTableName(featuregroupDTOS.get(0).getName(), featuregroupDTOS.get(0).getVersion()));
        stringBuilder.append(".`");
        stringBuilder.append(joinKey);
        stringBuilder.append("`=");
        stringBuilder.append(getTableName(featuregroupDTOS.get(i).getName(), featuregroupDTOS.get(i).getVersion()));
        stringBuilder.append(".`");
        stringBuilder.append(joinKey);
        stringBuilder.append("` AND ");
      }
      if (i != 0 && i == (featuregroupDTOS.size() - 1)) {
        stringBuilder.append(
            getTableName(featuregroupDTOS.get(0).getName(), featuregroupDTOS.get(0).getVersion()));
        stringBuilder.append(".`");
        stringBuilder.append(joinKey);
        stringBuilder.append("`=");
        stringBuilder.append(getTableName(featuregroupDTOS.get(i).getName(), featuregroupDTOS.get(i).getVersion()));
        stringBuilder.append(".`");
        stringBuilder.append(joinKey);
        stringBuilder.append("`");
      }
    }
    return new SQLJoinDTO(stringBuilder.toString(), featuregroupDTOS);
  }

  /**
   * Helper method that returns the column among a shared column between featuregroups that is most often marked as
   * 'primary' in the hive schema.
   *
   * @param commonCols       the list of columns shared between all featuregroups
   * @param featuregroupDTOS the list of featuregroups
   * @return the column among a shared column between featuregroups that is most often marked as 'primary'
   * in the hive schema
   */
  private static String getColumnThatIsPrimary(String[] commonCols, List<FeaturegroupDTO> featuregroupDTOS) {
    int[] primaryCounts = new int[commonCols.length];
    for (int i = 0; i < commonCols.length; i++) {
      int primaryCount = 0;
      for (FeaturegroupDTO featuregroupDTO : featuregroupDTOS) {
        for (FeatureDTO featureDTO : featuregroupDTO.getFeatures()) {
          if (featureDTO.getName().equalsIgnoreCase(commonCols[i]) && featureDTO.getPrimary())
            primaryCount++;
        }
      }
      primaryCounts[i] = primaryCount;
    }
    int maxPrimaryCount = 0;
    int maxPrimaryCountIndex = 0;
    for (int i = 0; i < primaryCounts.length; i++) {
      if (primaryCounts[i] > maxPrimaryCount) {
        maxPrimaryCount = primaryCounts[i];
        maxPrimaryCountIndex = i;
      }
    }
    return commonCols[maxPrimaryCountIndex];
  }

  /**
   * Method to infer a possible join column for a list of featuregroups
   *
   * @param featuregroupDTOS the list of featuregroups
   * @return the join column
   */
  public static String getJoinColumn(List<FeaturegroupDTO> featuregroupDTOS) {
    List<Set<String>> featureSets = new ArrayList<>();
    Set<String> completeFeatureSet = new HashSet<>();
    for (FeaturegroupDTO featuregroupDTO : featuregroupDTOS) {
      List<String> columnNames =
          featuregroupDTO.getFeatures().stream().map(feature -> feature.getName()).collect(Collectors.toList());
      Set<String> featureSet = new HashSet<>(columnNames);
      completeFeatureSet.addAll(columnNames);
      featureSets.add(featureSet);
    }
    for (Set<String> featureSet : featureSets) {
      completeFeatureSet.retainAll(featureSet);
    }
    if (completeFeatureSet.isEmpty()) {
      List<String> featuregroupStrings =
          featuregroupDTOS.stream()
              .map(featuregroup -> featuregroup.getName()).collect(Collectors.toList());
      String featuregroupsStr = StringUtils.join(featuregroupStrings, ", ");
      throw new IllegalArgumentException("Could not find any common columns in featuregroups to join on," +
          " searched through the following featuregroups: " + featuregroupsStr);
    }
    return getColumnThatIsPrimary(completeFeatureSet.toArray(new String[completeFeatureSet.size()]), featuregroupDTOS);
  }

  /**
   * Converts a map of (featuregroup to version) to a list of featuregroupDTOs
   *
   * @param featuregroupsAndVersions the map of (featuregroup to version)
   * @return a list of featuregroupDTOs
   */
  private static List<FeaturegroupDTO> convertFeaturegroupAndVersionToDTOs(
      Map<String, Integer> featuregroupsAndVersions) {
    List<FeaturegroupDTO> featuregroupDTOs = new ArrayList<>();
    for (Map.Entry<String, Integer> entry : featuregroupsAndVersions.entrySet()) {
      FeaturegroupDTO featuregroupDTO = new FeaturegroupDTO();
      featuregroupDTO.setName(entry.getKey());
      featuregroupDTO.setVersion(entry.getValue());
      featuregroupDTOs.add(featuregroupDTO);
    }
    return featuregroupDTOs;
  }

  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe. This method is used if the user
   * has itself provided a set of featuregroups where the features are located and should be queried from, it does not
   * infer the featuregroups.
   *
   * @param sparkSession             the spark session
   * @param features                 the list of features to get
   * @param featurestore             the featurestore to query
   * @param featuregroupsAndVersions a map of (featuregroup to version) where the featuregroups are located
   * @param joinKey                  the key to join on
   * @param jdbcArguments jdbc arguments for fetching the on-demand featuregroups
   * @param featuregroupsMetadata    metadata of the feature groups
   * @param online                   boolean flag whether the features should be fetched from the online featurestore
   * @return the resulting spark dataframe with the features
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  public static Dataset<Row> getFeatures(SparkSession sparkSession, List<String> features, String featurestore,
                                         Map<String, Integer> featuregroupsAndVersions, String joinKey,
                                         List<FeaturegroupDTO> featuregroupsMetadata,
                                         Map<String, Map<String, String>> jdbcArguments, Boolean online)
    throws FeaturegroupDoesNotExistError, StorageConnectorDoesNotExistError, HiveNotEnabled,
    OnlineFeaturestorePasswordNotFound, FeaturestoreNotFound, OnlineFeaturestoreUserNotFound, JAXBException,
    OnlineFeaturestoreNotEnabled {
    useFeaturestore(sparkSession, featurestore);
    useFeaturestore(sparkSession, featurestore);
    String featuresStr = StringUtils.join(features, ", ");
    List<String> featuregroupStrings = new ArrayList<>();
    List<FeaturegroupDTO> featuregroupDTOS = new ArrayList<>();
    for (Map.Entry<String, Integer> entry : featuregroupsAndVersions.entrySet()) {
      featuregroupStrings.add(getTableName(entry.getKey(), entry.getValue()));
      featuregroupDTOS.add(findFeaturegroup(featuregroupsMetadata, entry.getKey(), entry.getValue()));
    }
    String featuregroupStr = StringUtils.join(featuregroupStrings, ", ");
    List<FeaturegroupDTO> onDemandFeaturegroups =
        featuregroupDTOS.stream()
            .filter(fg -> fg.getFeaturegroupType() ==
                FeaturegroupType.ON_DEMAND_FEATURE_GROUP).collect(Collectors.toList());
    registerOnDemandFeaturegroupsAsTempTables(onDemandFeaturegroups, featurestore, jdbcArguments);
    if (featuregroupsAndVersions.size() == 1) {
      Map<String, String> featureToFeaturegroupMapping =
        getFeatureToFeaturegroupMapping(featuregroupDTOS, features, featurestore);
      String sqlStr = "SELECT " + featuresStr + " FROM " + featuregroupStr;
      Dataset<Row> result = logAndRunSQL(sparkSession, sqlStr, online, featurestore);
      return addProvenanceMetadataToDataFrame(result, featureToFeaturegroupMapping);
    } else {
      SQLJoinDTO sqlJoinDTO = getJoinStr(featuregroupDTOS, joinKey);
      String sqlStr = "SELECT " + featuresStr + " FROM " +
          getTableName(sqlJoinDTO.getFeaturegroupDTOS().get(0).getName(),
              sqlJoinDTO.getFeaturegroupDTOS().get(0).getVersion())
          + " " + sqlJoinDTO.getJoinStr();
      Map<String, String> featureToFeaturegroupMapping =
        getFeatureToFeaturegroupMapping(sqlJoinDTO.getFeaturegroupDTOS(), features, featurestore);
      Dataset<Row> result = logAndRunSQL(sparkSession, sqlStr, online, featurestore);
      return addProvenanceMetadataToDataFrame(result, featureToFeaturegroupMapping);
    }
  }
  
  /**
   * Adds provenance information to spark dataframe to track from which feature groups the features in the
   * dataframe originate.
   *
   * @param sparkDf the spark dataframe to add the provenance information to
   * @param featureToFeaturegroupMapping a mapping from feature to feature group
   * @return a spark dataframe with the provenance metadata
   */
  private static Dataset<Row> addProvenanceMetadataToDataFrame(Dataset<Row> sparkDf,
    Map<String, String> featureToFeaturegroupMapping) {
    for (Map.Entry<String, String> entry : featureToFeaturegroupMapping.entrySet()) {
      Metadata metadata = getColumnMetadata(sparkDf.schema(), entry.getKey());
      int lastIndexOf = entry.getValue().lastIndexOf(Constants.UNDERSCORE_DELIMITER);
      String featuregroupName = entry.getValue().substring(0, lastIndexOf);
      String featuregroupVersion = entry.getValue().substring(lastIndexOf+1);
      MetadataBuilder metadataBuilder =
        new MetadataBuilder().putString(Constants.TRAINING_DATASET_PROVENANCE_FEATUREGROUP, featuregroupName)
          .putString(Constants.TRAINING_DATASET_PROVENANCE_VERSION, featuregroupVersion);
      if(metadata.contains(Constants.TRAINING_DATASET_PROVENANCE_COMMENT)) {
        metadataBuilder = metadataBuilder.putString(Constants.TRAINING_DATASET_PROVENANCE_COMMENT,
          metadata.getString(Constants.TRAINING_DATASET_PROVENANCE_COMMENT));
      }
      sparkDf = sparkDf.withColumn(entry.getKey(), col(entry.getKey()).as("", metadataBuilder.build()));
    }
    return sparkDf;
  }
  
  /**
   * Get metadata of a specific column in a spark dataframe
   *
   * @param sparkSchema the schema of the spark dataframe
   * @param columnName name of the column (feature) in the spark dataframe to get the metadata of
   * @return metadata of the column
   */
  private static Metadata getColumnMetadata(StructType sparkSchema, String columnName) {
    for (int i = 0; i < sparkSchema.fields().length; i++) {
      StructField field = sparkSchema.fields()[i];
      if(field.name().equalsIgnoreCase(columnName)){
        return field.metadata();
      }
    }
    throw new IllegalArgumentException("Feature not found: " + columnName);
  }
  
  /**
   * Constructs a map to convert a feature into the corresponding featuregroup. Used for training dataset provenance
   *
   * @param featuregroups list of featuregroups
   * @param features list of features
   * @param featurestore featurestore
   * @return map to convert from feature to featuregroup that the feature belongs to
   */
  private static Map<String, String> getFeatureToFeaturegroupMapping(List<FeaturegroupDTO> featuregroups,
    List<String> features, String featurestore) {
    Map<String, String> mapping = new HashMap<>();
    for (String feature : features) {
      FeaturegroupDTO featuregroupDTO = findFeature(feature, featurestore, featuregroups);
      mapping.put(getFeatureShortName(feature), getTableName(featuregroupDTO.getName(), featuregroupDTO.getVersion()));
    }
    return mapping;
  }
  
  /**
   * Converts a feature name into a short name. If the feature name is featuregroup.feature then it will return
   * simply the feature name. If the feature name does not include the featuregroup, this method does nothing.
   *
   * @param featureName the original feature name
   * @return the short version of the feature name
   */
  private static String getFeatureShortName(String featureName) {
    if (featureName.contains(Constants.DOT_DELIMITER)) {
      return featureName.split("\\.")[1];
    }
    return featureName;
  }
  
  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe. This method will infer
   * in which featuregroups the features belong using metadata from the metastore
   *
   * @param sparkSession          the spark session
   * @param features              the list of features to get
   * @param featurestore          the featurestore to query
   * @param featuregroupsMetadata metadata about the featuregroups in the featurestore
   * @param joinKey               the key to join on
   * @param jdbcArguments         jdbc arguments for fetching on-demand featuregroups
   * @param online                whether to read from the online feature group or offline
   * @return the resulting spark dataframe with the features
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  public static Dataset<Row> getFeatures(SparkSession sparkSession, List<String> features,
                                         String featurestore, List<FeaturegroupDTO> featuregroupsMetadata,
                                         String joinKey, Map<String, Map<String, String>> jdbcArguments, Boolean online)
    throws FeaturegroupDoesNotExistError, HiveNotEnabled, StorageConnectorDoesNotExistError,
    OnlineFeaturestorePasswordNotFound, FeaturestoreNotFound, OnlineFeaturestoreUserNotFound, JAXBException,
    OnlineFeaturestoreNotEnabled {
    useFeaturestore(sparkSession, featurestore);
    String featuresStr = StringUtils.join(features, ", ");
    List<String> featuregroupStrings =
        featuregroupsMetadata.stream()
            .map(fg -> getTableName(fg.getName(), fg.getVersion())).collect(Collectors.toList());
    List<FeaturegroupDTO> onDemandFeaturegroups =
        featuregroupsMetadata.stream()
            .filter(fg -> fg.getFeaturegroupType() ==
                FeaturegroupType.ON_DEMAND_FEATURE_GROUP).collect(Collectors.toList());
    registerOnDemandFeaturegroupsAsTempTables(onDemandFeaturegroups, featurestore, jdbcArguments);
    String featuregroupStr = StringUtils.join(featuregroupStrings, ", ");
    if (featuregroupsMetadata.size() == 1) {
      String sqlStr = "SELECT " + featuresStr + " FROM " + featuregroupStr;
      Map<String, String> featureToFeaturegroupMapping =
        getFeatureToFeaturegroupMapping(featuregroupsMetadata, features, featurestore);
      Dataset<Row> result = logAndRunSQL(sparkSession, sqlStr, online, featurestore);
      return addProvenanceMetadataToDataFrame(result, featureToFeaturegroupMapping);
    } else {
      SQLJoinDTO sqlJoinDTO = getJoinStr(featuregroupsMetadata, joinKey);
      String sqlStr = "SELECT " + featuresStr + " FROM " +
          getTableName(sqlJoinDTO.getFeaturegroupDTOS().get(0).getName(),
              sqlJoinDTO.getFeaturegroupDTOS().get(0).getVersion())
          + " " + sqlJoinDTO.getJoinStr();
      Map<String, String> featureToFeaturegroupMapping =
        getFeatureToFeaturegroupMapping(sqlJoinDTO.getFeaturegroupDTOS(), features, featurestore);
      Dataset<Row> result = logAndRunSQL(sparkSession, sqlStr, online, featurestore);
      return addProvenanceMetadataToDataFrame(result, featureToFeaturegroupMapping);
    }
  }


  /**
   * Runs an SQL query on the project's featurestore
   *
   * @param sparkSession the spark session
   * @param query        the query to run
   * @param featurestore the featurestore to query
   * @param online       whether to read from the online feature group or offline
   * @return the resulting Spark dataframe
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  public static Dataset<Row> queryFeaturestore(SparkSession sparkSession, String query, String featurestore,
    Boolean online)
    throws OnlineFeaturestorePasswordNotFound, FeaturestoreNotFound, OnlineFeaturestoreUserNotFound, JAXBException,
    OnlineFeaturestoreNotEnabled {
    useFeaturestore(sparkSession, featurestore);
    return logAndRunSQL(sparkSession, query, online, featurestore);
  }

  /**
   * Runs an SQL query with SparkSQL and logs it
   *
   * @param sparkSession the spark session
   * @param sqlStr       the query to run
   * @return the resulting spark dataframe
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  private static Dataset<Row> logAndRunSQL(SparkSession sparkSession, String sqlStr, Boolean online,
    String featurestore) throws JAXBException, FeaturestoreNotFound, OnlineFeaturestoreUserNotFound,
    OnlineFeaturestorePasswordNotFound, OnlineFeaturestoreNotEnabled {
    if(online == null || !online) {
      LOG.log(Level.INFO, "Running sql: " + sqlStr + " against offline feature store");
      return sparkSession.sql(sqlStr);
    } else {
      LOG.log(Level.INFO, "Running sql: " + sqlStr + " against online feature store");
      FeaturestoreMetadataDTO featurestoreMetadataDTO =
        new FeaturestoreReadMetadata().setFeaturestore(featurestore).read();
      if(online && (!(featurestoreMetadataDTO.getSettings().getOnlineFeaturestoreEnabled())
        || (!featurestoreMetadataDTO.getFeaturestore().getOnlineEnabled()))) {
        throw new OnlineFeaturestoreNotEnabled("Online feature store is not enabled for this cluster or project. " +
          "Talk with an administrator to enable it.");
      }
      FeaturestoreJdbcConnectorDTO featurestoreJdbcConnectorDTO = doGetOnlineFeaturestoreJdbcConnector(featurestore,
        new FeaturestoreReadMetadata().setFeaturestore(featurestore).read());
      return readJdbcDataFrame(sparkSession, featurestoreJdbcConnectorDTO, "(" + sqlStr + ") tmp");
    }
  }

  /**
   * Filters a list of featuregroups based on a user-provided map of featuregroup to version
   *
   * @param featuregroupsAndVersions the map of featuregroup to version
   * @param featuregroupsMetadata    the list of featuregroups to filter
   * @return filtered list of featuregroups
   */
  public static List<FeaturegroupDTO> filterFeaturegroupsBasedOnMap(
      Map<String, Integer> featuregroupsAndVersions, List<FeaturegroupDTO> featuregroupsMetadata) {
    return featuregroupsMetadata.stream().filter(fgm -> featuregroupsAndVersions.get(fgm.getName()) != null
            && fgm.getVersion().equals(featuregroupsAndVersions.get(fgm.getName()))).collect(Collectors.toList());
  }
  
  /**
   * Makes a REST call to Hopsworks to get the online featurestore connector for a particular featurestore and user
   * (unless the connector is in the metadata cache)
   *
   * @param featurestore the name of the featurestore
   * @param featurestoreMetadataDTO featurestore metadata
   * @return the JDBC connector
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   */
  public static FeaturestoreJdbcConnectorDTO doGetOnlineFeaturestoreJdbcConnector(String featurestore,
    FeaturestoreMetadataDTO featurestoreMetadataDTO)
    throws JAXBException, FeaturestoreNotFound {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    if(Strings.isNullOrEmpty(featurestore)){
      throw new IllegalArgumentException("Featurestore Parameter Cannot be Null.");
    }
    if(featurestoreMetadataDTO != null && featurestoreMetadataDTO.getOnlineFeaturestoreConnector() != null) {
      return featurestoreMetadataDTO.getOnlineFeaturestoreConnector();
    } else {
      return FeaturestoreRestClient.getOnlineFeaturestoreJdbcConnectorRest(featurestore);
    }
  }

  /**
   * Converts a spark schema field into a FeatureDTO
   *
   * @param field       the field to convert
   * @param primaryKeys the name of the primary key for the featuregroup where the feature belongs
   * @param partitionBy the features to partition a feature group on
   * @param online      boolean flag whether to also infer the online type
   * @param onlineTypes map of (featureName --> type). By default, spark datatypes will be used to infer MySQL
   *                    datatypes when creating MySQL tables in the Online Feature Store, but this behaviour can be
   *                    overridden by providing explicit feature types through this map.
   * @return the converted featureDTO
   */
  private static FeatureDTO convertFieldToFeature(StructField field, List<String> primaryKeys, List<String> partitionBy,
    Boolean online, Map<String, String> onlineTypes) {
    String featureName = field.name();
    String featureType = field.dataType().catalogString();
    String featureDesc = "";
    String onlineType = "";
    Boolean primary = false;
    if (primaryKeys != null && primaryKeys.contains(featureName)) {
      primary = true;
    }
    if (field.metadata() != null && field.metadata().contains(Constants.JSON_FEATURE_DESCRIPTION)) {
      featureDesc = field.metadata().getString(Constants.JSON_FEATURE_DESCRIPTION);
    }
    if (field.metadata() != null && !field.metadata().contains(Constants.JSON_FEATURE_DESCRIPTION) &&
        field.getComment().isDefined()) {
      featureDesc = field.getComment().get();
    }
    if (featureDesc.isEmpty()) {
      featureDesc = "-"; //comment should be non-empty
    }
    Boolean partition = false;
    if(partitionBy != null && partitionBy.contains(featureName)){
      partition = true;
    }
    if(online) {
      if(onlineTypes != null && onlineTypes.containsKey(featureName)) {
        onlineType = onlineTypes.get(featureName);
      } else {
        onlineType = convertHiveTypeToMySQL(field.dataType().catalogString());
      }
    }
    return new FeatureDTO(featureName, featureType, featureDesc, primary, partition, onlineType);
  }
  
  /**
   * Converts a HIVE data type to a MYSQL data type (helper method, not guaranteed to work always)
   *
   * @param hiveDataType the hive datatype to convert
   * @return the MYSQL data type
   */
  private static String convertHiveTypeToMySQL(String hiveDataType) {
    if(hiveDataType.equalsIgnoreCase(Constants.HIVE_SMALLINT_TYPE)){
      return Constants.MYSQL_SMALLINT_TYPE;
    }
    if(hiveDataType.equalsIgnoreCase(Constants.HIVE_INT_TYPE)){
      return Constants.MYSQL_INTEGER_TYPE;
    }
    if(hiveDataType.equalsIgnoreCase(Constants.HIVE_BIGINT_TYPE)) {
      return Constants.MYSQL_BIGINT_TYPE;
    }
    if(hiveDataType.equalsIgnoreCase(Constants.HIVE_INTERVAL_TYPE)) {
      return Constants.MYSQL_DATE_TYPE;
    }
    if(hiveDataType.equalsIgnoreCase(Constants.HIVE_STRING_TYPE)) {
      return Constants.MYSQL_VARCHAR_1000_TYPE;
    }
    if(hiveDataType.equalsIgnoreCase(Constants.HIVE_VARCHAR_TYPE)){
      return Constants.MYSQL_VARCHAR_1000_TYPE;
    }
    if(hiveDataType.equalsIgnoreCase(Constants.HIVE_BOOLEAN_TYPE)) {
      return Constants.MYSQL_TINYINT_TYPE;
    }
    if(hiveDataType.equalsIgnoreCase(Constants.HIVE_BINARY_TYPE)) {
      return Constants.MYSQL_BLOB_TYPE;
    }
    if(Constants.MYSQL_DATA_TYPES.contains(hiveDataType.toUpperCase())) {
      return hiveDataType;
    }
    throw new IllegalArgumentException("Conversion of data type: " + hiveDataType + " to a valid MySQL datatype " +
      "failed. Please explicitly provide the type through the argument 'onlineTypes'");
  }

  /**
   * Parses a spark schema into a list of FeatureDTOs
   *
   * @param sparkSchema the spark schema to parse
   * @param primaryKeys the name of the primary keys for the featuregroup where the feature belongs
   * @param partitionBy the features to partition a feature group on
   * @param online      boolean flag whether to also infer the online type
   * @param onlineTypes map of (featureName to type). By default, spark datatypes will be used to infer MySQL
   *                    datatypes when creating MySQL tables in the Online Feature Store, but this behaviour can be
   *                    overridden by providing explicit feature types through this map.
   * @return a list of feature DTOs
   */
  public static List<FeatureDTO> parseSparkFeaturesSchema(StructType sparkSchema, List<String> primaryKeys,
    List<String> partitionBy, Boolean online, Map<String, String> onlineTypes) {
    StructField[] fieldsList = sparkSchema.fields();
    List<FeatureDTO> features = new ArrayList<>();
    for (int i = 0; i < fieldsList.length; i++) {
      features.add(convertFieldToFeature(fieldsList[i], primaryKeys, partitionBy, online, onlineTypes));
    }
    return features;
  }

  /**
   * Utility method for getting the default primary keys of a featuregroup that is to be created if no other primary
   * key have been provided by the user
   *
   * @param featuregroupDf the spark dataframe to create the featuregroup from
   * @return the name of the column that is the primary key
   */
  public static List<String> getDefaultPrimaryKeys(Dataset<Row> featuregroupDf) {
    List<String> primaryKeys = new ArrayList<>();
    primaryKeys.add(featuregroupDf.dtypes()[0]._1);
    return primaryKeys;
  }

  /**
   * Utility method for validating a primary key provided by a user for a new featuregroup
   *
   * @param featuregroupDf the spark dataframe to create the featuregroup from
   * @param primaryKeys    the provided primary keys
   * @return true or false depending on if primary key is valid or not
   * @throws InvalidPrimaryKeyForFeaturegroup InvalidPrimaryKeyForFeaturegroup
   */
  public static Boolean validatePrimaryKeys(Dataset<Row> featuregroupDf, List<String> primaryKeys)
      throws InvalidPrimaryKeyForFeaturegroup {
    List<String> columns = new ArrayList<>();
    for (int i = 0; i < featuregroupDf.dtypes().length; i++) {
      columns.add(featuregroupDf.dtypes()[i]._1.toLowerCase());
    }
    for (String pk: primaryKeys) {
      if (!columns.contains(pk.toLowerCase())) {
        throw new InvalidPrimaryKeyForFeaturegroup("Invalid primary Key: " + pk
          + ", the specified primary key does not exist among the available columns: " +
          StringUtils.join(",", columns));
      }
    }
    return true;
  }

  /**
   * Validates metadata provided by the user when creating new feature groups and training datasets
   *
   * @param name the name of the feature group/training dataset
   * @param dtypes the schema of the provided spark dataframe
   * @param description the description about the feature group/training dataset
   */
  public static void validateMetadata(String name, Tuple2<String, String>[] dtypes, String description) {
    Pattern namePattern = Pattern.compile("^[a-zA-Z0-9_]+$");
    if (name.length() > 256 || name.equals("") || !namePattern.matcher(name).matches())
      throw new IllegalArgumentException("Name of feature group/training dataset cannot be empty, " +
          "cannot exceed 256 characters, cannot contain hyphens ('-') " +
          "and must match the regular expression: ^[a-zA-Z0-9_]+$" +
          " the provided name: " + name + " is not valid");

    if (dtypes.length == 0)
      throw new IllegalArgumentException("Cannot create a feature group from an empty spark dataframe");

    for (int i = 0; i < dtypes.length; i++) {
      if (dtypes[i]._1.length() > 767 || dtypes[i]._1.equals("") || !namePattern.matcher(dtypes[i]._1).matches())
        throw new IllegalArgumentException("Name of feature column cannot be empty, cannot exceed 767 characters," +
            ", cannot contains hyphens ('-'), and must match the regular expression: ^[a-zA-Z0-9_]+$, " +
            "the provided feature name: " + dtypes[i]._1 +
            " is not valid");
    }

    if(description.length() > 2000)
      throw new IllegalArgumentException("Feature group/Training dataset description should not exceed " +
          "the maximum length of 2000 characters, " +
          "the provided description has length:" + description.length());
  }

  /**
   * Converts a list of FeatureDTOs into a JSONArray
   *
   * @param featureDTOS the list of FeatureDTOs to convert
   * @return a JSONArray with the features
   * @throws JAXBException JAXBException
   */
  public static JSONArray convertFeatureDTOsToJsonObjects(List<FeatureDTO> featureDTOS) throws JAXBException {
    JSONArray features = new JSONArray();
    for (FeatureDTO featureDTO : featureDTOS) {
      features.put(dtoToJson(featureMarshaller, featureDTO));
    }
    return features;
  }

  /**
   * Converts a generic DTO into a JSONObject
   *
   * @param marshaller the JAXB marshaller to use for the conversion
   * @param object     the object to convert
   * @return the converted JSONObject
   * @throws JAXBException JAXBException
   */
  public static JSONObject dtoToJson(Marshaller marshaller, Object object) throws JAXBException {
    if (object == null)
      return null;
    StringWriter sw = new StringWriter();
    marshaller.marshal(object, sw);
    return new JSONObject(sw.toString());
  }

  /**
   * Gets an unmarshaller for a specific JAXBContext
   *
   * @param jaxbContext the jaxb context to get the unmarshaller for
   * @return the unmarshaller
   * @throws JAXBException JAXBException
   */
  public static Unmarshaller getUnmarshaller(JAXBContext jaxbContext) throws JAXBException {
    Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
    unmarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
    unmarshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
    return unmarshaller;
  }

  /**
   * Converts a FeaturegroupDTO into a JSONObject
   *
   * @param featuregroupDTO the DTO to convert
   * @return the JSONObject
   * @throws JAXBException JAXBException
   */
  public static JSONObject convertFeaturegroupDTOToJsonObject(
    FeaturegroupDTO featuregroupDTO) throws JAXBException {
    return dtoToJson(featuregroupMarshaller, featuregroupDTO);
  }

  /**
   * Converts a TrainingDatasetDTO into a JSONObject
   *
   * @param trainingDatasetDTO the DTO to convert
   * @return the JSONObject
   * @throws JAXBException JAXBException
   */
  public static JSONObject convertTrainingDatasetDTOToJsonObject(
    TrainingDatasetDTO trainingDatasetDTO) throws JAXBException {
    return dtoToJson(trainingDatasetMarshaller, trainingDatasetDTO);
  }

  /**
   * Converts a FeatureCorrelationMatrixDTO into a JSONObject
   *
   * @param featureCorrelationMatrixDTO the DTO to convert
   * @return the JSONObject
   * @throws JAXBException JAXBException
   */
  public static JSONObject convertFeatureCorrelationMatrixDTOToJsonObject(
      FeatureCorrelationMatrixDTO featureCorrelationMatrixDTO) throws JAXBException {
    return dtoToJson(featureCorrelationMarshaller, featureCorrelationMatrixDTO);
  }

  /**
   * Converts a DescriptiveStatsDTO into a JSONObject
   *
   * @param descriptiveStatsDTO the DTO to convert
   * @return the JSON object
   * @throws JAXBException JAXBException
   */
  public static JSONObject convertDescriptiveStatsDTOToJsonObject(
      DescriptiveStatsDTO descriptiveStatsDTO) throws JAXBException {
    return dtoToJson(descriptiveStatsMarshaller, descriptiveStatsDTO);
  }

  /**
   * Converts a FeatureDistributionsDTO into a JSONObject
   *
   * @param featureDistributionsDTO the DTO to convert
   * @return the JSON object
   * @throws JAXBException JAXBException
   */
  public static JSONObject convertFeatureDistributionsDTOToJsonObject(
      FeatureDistributionsDTO featureDistributionsDTO) throws JAXBException {
    return dtoToJson(featureHistogramsMarshaller, featureDistributionsDTO);
  }

  /**
   * Converts a ClusterAnalysisDTO into a JSONObject
   *
   * @param clusterAnalysisDTO the DTO to convert
   * @return the JSON object
   * @throws JAXBException JAXBException
   */
  public static JSONObject convertClusterAnalysisDTOToJsonObject(
      ClusterAnalysisDTO clusterAnalysisDTO) throws JAXBException {
    return dtoToJson(clusteranalysisMarshaller, clusterAnalysisDTO);
  }
  
  /**
   * Parses JDBC Connector JSON into a DTO
   *
   * @param jdbcConnectorJson the JSON to parse
   * @return the DTO
   * @throws JAXBException JAXBException
   */
  public static FeaturestoreJdbcConnectorDTO parseJdbcConnectorJson(JSONObject jdbcConnectorJson)
    throws JAXBException {
    Unmarshaller unmarshaller = getUnmarshaller(jdbcConnectorJAXBContext);
    StreamSource json = new StreamSource(new StringReader(jdbcConnectorJson.toString()));
    return unmarshaller.unmarshal(json, FeaturestoreJdbcConnectorDTO.class).getValue();
  }

  /**
   * Parses FeaturestoreMetadata JSON into a DTO
   *
   * @param featurestoreMetadata the JSON to parse
   * @return the DTO
   * @throws JAXBException JAXBException
   */
  public static FeaturestoreMetadataDTO parseFeaturestoreMetadataJson(JSONObject featurestoreMetadata)
      throws JAXBException {
    Unmarshaller unmarshaller = getUnmarshaller(featurestoreMetadataJAXBContext);
    StreamSource json = new StreamSource(new StringReader(featurestoreMetadata.toString()));
    return unmarshaller.unmarshal(json, FeaturestoreMetadataDTO.class).getValue();
  }

  /**
   * Parses training dataset JSON into a DTO
   *
   * @param trainingDatasetJson the JSON to parse
   * @return the DTO
   * @throws JAXBException JAXBException
   */
  public static TrainingDatasetDTO parseTrainingDatasetJson(JSONObject trainingDatasetJson)
      throws JAXBException {
    Unmarshaller unmarshaller = getUnmarshaller(trainingDatasetJAXBContext);
    StreamSource json = new StreamSource(new StringReader(trainingDatasetJson.toString()));
    return unmarshaller.unmarshal(json, TrainingDatasetDTO.class).getValue();
  }

  /**
   * Filters a spark dataframe to only keep numeric columns
   *
   * @param sparkDf the spark dataframe to filter
   * @return the filtered dataframe with only numeric columns
   */
  private static Dataset<Row> filterSparkDfNumeric(Dataset<Row> sparkDf) {
    StructField[] fields = sparkDf.schema().fields();
    List<Column> numericColumnNames = new ArrayList<>();
    for (int i = 0; i < fields.length; i++) {
      if (fields[i].dataType() instanceof NumericType) {
        numericColumnNames.add(col(fields[i].name()));
      }
    }
    Column[] numericColumnNamesArr = new Column[numericColumnNames.size()];
    numericColumnNames.toArray(numericColumnNamesArr);
    return sparkDf.select(numericColumnNamesArr);
  }

  /**
   * Computes descriptive statistics for a spark dataframe using spark
   *
   * @param sparkDf the dataframe to compute statistics for
   * @return the computed statistics
   */
  private static DescriptiveStatsDTO computeDescriptiveStatistics(Dataset<Row> sparkDf) {
    String[] rawStatsArray = (String[]) sparkDf.describe().toJSON().collect();
    List<JSONObject> rawStatsObjects = new ArrayList<>();
    Set<String> columnNames = new HashSet<>();
    for (int i = 0; i < rawStatsArray.length; i++) {
      JSONObject rawStatObj = new JSONObject(rawStatsArray[i]);
      rawStatsObjects.add(rawStatObj);
      columnNames.addAll(rawStatObj.keySet());
    }
    List<DescriptiveStatsMetricValuesDTO> descriptiveStatsMetricValuesDTOList = new ArrayList<>();
    for (String colName : columnNames) {
      if (!colName.equals(Constants.DESCRIPTIVE_STATS_SUMMARY_COL)) {
        DescriptiveStatsMetricValuesDTO descriptiveStatsMetricValuesDTO = new DescriptiveStatsMetricValuesDTO();
        List<DescriptiveStatsMetricValueDTO> descriptiveStatsMetricValueDTOList = new ArrayList<>();
        for (int i = 0; i < rawStatsObjects.size(); i++) {
          JSONObject rawStatObj = rawStatsObjects.get(i);
          if (rawStatObj.has(colName)) {
            Float value;
            try {
              value = Float.parseFloat(rawStatObj.getString(colName));
              if (value.isNaN() || value.isInfinite())
                value = null;
            } catch (NullPointerException | NumberFormatException e) {
              value = null;
            }
            DescriptiveStatsMetricValueDTO descriptiveStatsMetricValueDTO = new DescriptiveStatsMetricValueDTO();
            descriptiveStatsMetricValueDTO.setValue(value);
            descriptiveStatsMetricValueDTO.setMetricName(rawStatObj.getString(Constants.DESCRIPTIVE_STATS_SUMMARY_COL));
            descriptiveStatsMetricValueDTOList.add(descriptiveStatsMetricValueDTO);
          }
        }
        descriptiveStatsMetricValuesDTO.setFeatureName(colName);
        descriptiveStatsMetricValuesDTO.setMetricValues(descriptiveStatsMetricValueDTOList);
        descriptiveStatsMetricValuesDTOList.add(descriptiveStatsMetricValuesDTO);
      }
    }
    DescriptiveStatsDTO descriptiveStatsDTO = new DescriptiveStatsDTO();
    descriptiveStatsDTO.setDescriptiveStats(descriptiveStatsMetricValuesDTOList);
    return descriptiveStatsDTO;
  }

  /**
   * Computes cluster analysis statistics for a spark dataframe
   *
   * @param sparkDf  the spark dataframe to compute statistics for
   * @param clusters the number of clusters to use in the statistics
   * @return the computed statistics
   */
  private static ClusterAnalysisDTO computeClusterAnalysis(Dataset<Row> sparkDf, int clusters) {
    Dataset<Row> sparkDf1 = assembleColumnsIntoVector(sparkDf, Constants.CLUSTERING_ANALYSIS_INPUT_COLUMN);
    KMeans kmeans = new KMeans();
    kmeans.setK(clusters);
    kmeans.setSeed(1L);
    kmeans.setMaxIter(20);
    kmeans.setFeaturesCol(Constants.CLUSTERING_ANALYSIS_INPUT_COLUMN);
    kmeans.setPredictionCol(Constants.CLUSTERING_ANALYSIS_OUTPUT_COLUMN);
    KMeansModel kMeansModel = kmeans.fit(sparkDf1.select(Constants.CLUSTERING_ANALYSIS_INPUT_COLUMN));
    Dataset<Row> sparkDf2 = kMeansModel.transform(sparkDf1);
    Column[] cols = new Column[2];
    cols[0] = col(Constants.CLUSTERING_ANALYSIS_INPUT_COLUMN);
    cols[1] = col(Constants.CLUSTERING_ANALYSIS_OUTPUT_COLUMN);
    Dataset<Row> sparkDf3 = sparkDf2.select(cols);
    Dataset<Row> sparkDf4;
    Long count = sparkDf3.count();
    if (count < Constants.CLUSTERING_ANALYSIS_SAMPLE_SIZE)
      sparkDf4 = sparkDf3;
    else
      sparkDf4 = sparkDf3.sample(true,
          ((float) Constants.CLUSTERING_ANALYSIS_SAMPLE_SIZE) / ((float) count));
    PCA pca = new PCA();
    pca.setK(2);
    pca.setInputCol(Constants.CLUSTERING_ANALYSIS_INPUT_COLUMN);
    pca.setOutputCol(Constants.CLUSTERING_ANALYSIS_PCA_COLUMN);
    PCAModel pcaModel = pca.fit(sparkDf4);
    cols[0] = col(Constants.CLUSTERING_ANALYSIS_PCA_COLUMN);
    cols[1] = col(Constants.CLUSTERING_ANALYSIS_OUTPUT_COLUMN);
    Dataset<Row> sparkDf5 = pcaModel.transform(sparkDf4).select(cols);
    Dataset<Row> sparkDf6 = sparkDf5.withColumnRenamed(
        Constants.CLUSTERING_ANALYSIS_PCA_COLUMN,
        Constants.CLUSTERING_ANALYSIS_FEATURES_COLUMN
    );
    Dataset<Row> sparkDf7 = sparkDf6.withColumnRenamed(
        Constants.CLUSTERING_ANALYSIS_OUTPUT_COLUMN,
        Constants.CLUSTERING_ANALYSIS_CLUSTERS_OUTPUT_COLUMN);
    String[] jsonStrResult = (String[]) sparkDf7.toJSON().collect();
    ClusterAnalysisDTO clusterAnalysisDTO = new ClusterAnalysisDTO();
    List<ClusterDTO> clusterDTOs = new ArrayList<>();
    List<DatapointDTO> datapointDTOs = new ArrayList<>();
    for (int i = 0; i < jsonStrResult.length; i++) {
      JSONObject jsonObj = new JSONObject(jsonStrResult[i]);
      JSONObject featuresObj = jsonObj.getJSONObject(Constants.CLUSTERING_ANALYSIS_FEATURES_COLUMN);
      int cluster = jsonObj.getInt(Constants.CLUSTERING_ANALYSIS_CLUSTERS_OUTPUT_COLUMN);
      JSONArray dimensions = featuresObj.getJSONArray(Constants.CLUSTERING_ANALYSIS_VALUES_COLUMN);
      DatapointDTO datapointDTO = new DatapointDTO();
      datapointDTO.setDatapointName(Integer.toString(i));
      try {
        Float firstDimension = (float) dimensions.getDouble(0);
        Float secondDimension = (float) dimensions.getDouble(1);
        if (firstDimension.isInfinite() || firstDimension.isNaN()) {
          firstDimension = 0.0f;
        }
        if (secondDimension.isNaN() || secondDimension.isNaN()) {
          secondDimension = 0.0f;
        }
        datapointDTO.setFirstDimension(firstDimension);
        datapointDTO.setSecondDimension(secondDimension);
      } catch (ClassCastException e) {
        datapointDTO.setFirstDimension(0.0f);
        datapointDTO.setSecondDimension(0.0f);
      }
      ClusterDTO clusterDTO = new ClusterDTO();
      clusterDTO.setCluster(cluster);
      clusterDTO.setDatapointName(Integer.toString(i));
      datapointDTOs.add(datapointDTO);
      clusterDTOs.add(clusterDTO);
    }
    clusterAnalysisDTO.setClusters(clusterDTOs);
    clusterAnalysisDTO.setDataPoints(datapointDTOs);
    return clusterAnalysisDTO;
  }

  /**
   * Computes feature correlation statistics for a spark dataframe
   *
   * @param sparkDf           the spark dataframe to compute correlation statistics for
   * @param correlationMethod the method to use for computing the correlations
   * @return the computed statistics
   */
  private static FeatureCorrelationMatrixDTO computeCorrMatrix(Dataset<Row> sparkDf, String correlationMethod) {
    int numberOfColumns = sparkDf.dtypes().length;
    if (numberOfColumns == 0) {
      throw new IllegalArgumentException(
          "The provided spark dataframe does not contain any numeric columns.\n" +
              "Cannot compute feature correlation on categorical columns. \n The numeric datatypes are:" +
              StringUtils.join(Constants.NUMERIC_SPARK_TYPES, ",") + " \n " +
              "The number of numeric datatypes in the provided dataframe is: " + numberOfColumns +
              "(" + Arrays.toString(sparkDf.dtypes()) + ")");
    }
    if (numberOfColumns == 1) {
      throw new IllegalArgumentException(
          "The provided spark dataframe only contains one numeric column.\n" +
              "Cannot compute feature correlation on just one column. \n The numeric datatypes are:" +
              StringUtils.join(Constants.NUMERIC_SPARK_TYPES, ",") + " \n " +
              "The number of numeric datatypes in the provided dataframe is: " + numberOfColumns +
              "(" + Arrays.toString(sparkDf.dtypes()) + ")");
    }
    if (numberOfColumns > Constants.MAX_CORRELATION_MATRIX_COLUMNS) {
      throw new IllegalArgumentException(
          "The provided spark dataframe have " + numberOfColumns +
              " columns, which exceeds the maximum number of columns: " + Constants.MAX_CORRELATION_MATRIX_COLUMNS +
              ". This is due to scalability reasons (number of correlatons grows quadratically with the " +
              "number of columns");
    }
    Dataset<Row> sparkDf1 = assembleColumnsIntoVector(sparkDf, Constants.CORRELATION_ANALYSIS_INPUT_COLUMN);
    Row firstRow = Correlation.corr(sparkDf1, Constants.CORRELATION_ANALYSIS_INPUT_COLUMN, correlationMethod).head();
    DenseMatrix correlationMatrix = (DenseMatrix) firstRow.get(0);
    FeatureCorrelationMatrixDTO featureCorrelationMatrixDTO = new FeatureCorrelationMatrixDTO();
    List<FeatureCorrelationDTO> featureCorrelationDTOList = new ArrayList<>();
    int noCorrelationMatrixColumns = correlationMatrix.numCols();
    int noCorrelationMatrixRows = correlationMatrix.numRows();
    StructField[] fields = sparkDf1.schema().fields();
    for (int i = 0; i < noCorrelationMatrixColumns; i++) {
      String featureName = fields[i].name();
      FeatureCorrelationDTO featureCorrelationDTO = new FeatureCorrelationDTO();
      List<CorrelationValueDTO> correlationValueDTOList = new ArrayList<>();
      featureCorrelationDTO.setFeatureName(featureName);
      for (int j = 0; j < noCorrelationMatrixRows; j++) {
        CorrelationValueDTO correlationValueDTO = new CorrelationValueDTO();
        String featureName2 = fields[j].name();
        correlationValueDTO.setFeatureName(featureName2);
        try {
          Float corr = (float) correlationMatrix.apply(i, j);
          if (corr.isNaN() || corr.isInfinite())
            corr = 0.0f;
          correlationValueDTO.setCorrelation(corr);
        } catch (ClassCastException e) {
          correlationValueDTO.setCorrelation(0.0f);
        }
        correlationValueDTOList.add(correlationValueDTO);
      }
      featureCorrelationDTO.setCorrelationValues(correlationValueDTOList);
      featureCorrelationDTOList.add(featureCorrelationDTO);
    }
    featureCorrelationMatrixDTO.setFeatureCorrelations(featureCorrelationDTOList);
    return featureCorrelationMatrixDTO;
  }

  /**
   * A method for assembling all columns in a spark dataframe into a DenseVector
   *
   * @param sparkDf the sparkdataframe to assemble columns for
   * @param colName the output column name of the DenseVector column
   * @return a spark dataframe with an extra column containing the assembled columns in a DenseVector
   */
  private static Dataset<Row> assembleColumnsIntoVector(Dataset<Row> sparkDf, String colName) {
    List<String> numericColumns = new ArrayList<>();
    for (int i = 0; i < sparkDf.dtypes().length; i++) {
      numericColumns.add(sparkDf.dtypes()[i]._1);
    }
    String[] numericColumnNamesArr = new String[numericColumns.size()];
    numericColumns.toArray(numericColumnNamesArr);
    VectorAssembler vectorAssembler = new VectorAssembler();
    vectorAssembler.setInputCols(numericColumnNamesArr);
    vectorAssembler.setOutputCol(colName);
    return vectorAssembler.transform(sparkDf);
  }

  /**
   * Computes feature histogram statistics for all columns in a spark dataframe
   *
   * @param sparkDf the spark dataframe to compute statistics for
   * @param numBins the number of bins to use for the histograms
   * @return the computed statistics
   * @throws SparkDataTypeNotRecognizedError if the spark datatype is not recognized
   */
  private static FeatureDistributionsDTO computeFeatureHistograms(Dataset<Row> sparkDf, int numBins)
      throws SparkDataTypeNotRecognizedError {
    List<FeatureDistributionDTO> featureDistributionDTOS = new ArrayList<>();
    for (int i = 0; i < sparkDf.schema().fields().length; i++) {
      Tuple2<double[], long[]> colHist = null;
      if (sparkDf.schema().fields()[i].dataType() instanceof IntegerType) {
        colHist = sparkDf.select(sparkDf.dtypes()[i]._1).
            toJavaRDD().map(x -> Double.valueOf(x.getInt(0))).mapToDouble(x -> x).histogram(numBins);
      }
      if (sparkDf.schema().fields()[i].dataType() instanceof DecimalType) {
        colHist = sparkDf.select(sparkDf.dtypes()[i]._1).
            toJavaRDD().map(x -> x.getDecimal(0).doubleValue()).mapToDouble(x -> x).histogram(numBins);
      }
      if (sparkDf.schema().fields()[i].dataType() instanceof DoubleType) {
        colHist = sparkDf.select(sparkDf.dtypes()[i]._1).
            toJavaRDD().map(x -> x.getDouble(0)).mapToDouble(x -> x).histogram(numBins);
      }
      if (sparkDf.schema().fields()[i].dataType() instanceof FloatType) {
        colHist = sparkDf.select(sparkDf.dtypes()[i]._1).
            toJavaRDD().map(x -> (double) x.getFloat(0)).mapToDouble(x -> x).histogram(numBins);
      }
      if (sparkDf.schema().fields()[i].dataType() instanceof LongType) {
        colHist = sparkDf.select(sparkDf.dtypes()[i]._1).
            toJavaRDD().map(x -> (double) x.getLong(0)).mapToDouble(x -> x).histogram(numBins);
      }
      if (sparkDf.schema().fields()[i].dataType() instanceof ShortType) {
        colHist = sparkDf.select(sparkDf.dtypes()[i]._1).
            toJavaRDD().map(x -> (double) x.getShort(0)).mapToDouble(x -> x).histogram(numBins);
      }
      if (colHist == null)
        throw new SparkDataTypeNotRecognizedError("Could not parse the spark datatypes to compute feature histograms");
      double[] bins = colHist._1;
      long[] frequencies = colHist._2;
      FeatureDistributionDTO featureDistributionDTO = new FeatureDistributionDTO();
      featureDistributionDTO.setFeatureName(sparkDf.dtypes()[i]._1);
      List<HistogramBinDTO> histogramBinDTOList = new ArrayList<>();
      for (int j = 0; j < frequencies.length; j++) {
        HistogramBinDTO histogramBinDTO = new HistogramBinDTO();
        histogramBinDTO.setFrequency((int) frequencies[j]);
        double bin = bins[j + 1];
        histogramBinDTO.setBin(Double.toString(bin));
        histogramBinDTOList.add(histogramBinDTO);
      }
      featureDistributionDTO.setFrequencyDistribution(histogramBinDTOList);
      featureDistributionDTOS.add(featureDistributionDTO);
    }
    FeatureDistributionsDTO featureDistributionsDTO = new FeatureDistributionsDTO();
    featureDistributionsDTO.setFeatureDistributions(featureDistributionDTOS);
    return featureDistributionsDTO;
  }

  /**
   * Helper function that computes statistics of a featuregroup or training dataset using spark
   *
   * @param name                  the featuregroup or training dataset to update statistics for
   * @param sparkSession          the spark session
   * @param sparkDf               If a spark df is provided it will be used to compute statistics,
   *                              otherwise the dataframe of the
   *                              featuregroup will be fetched dynamically from the featurestore
   * @param featurestore          the featurestore where the featuregroup or training dataset resides
   * @param version               the version of the featuregroup/training dataset (defaults to 1)
   * @param descriptiveStatistics a boolean flag whether to compute descriptive statistics (min,max,mean etc)
   *                              for the featuregroup/training dataset
   * @param featureCorrelation    a boolean flag whether to compute a feature correlation matrix for the numeric columns
   *                              in the featuregroup/training dataset
   * @param featureHistograms     a boolean flag whether to compute histograms for the numeric columns in the
   *                              featuregroup/training dataset
   * @param clusterAnalysis       a boolean flag whether to compute cluster analysis for the numeric columns in the
   *                              featuregroup/training dataset
   * @param statColumns           a list of columns to compute statistics for (defaults to all columns that are numeric)
   * @param numBins               number of bins to use for computing histograms
   * @param numClusters           the number of clusters to use for cluster analysis (k-means)
   * @param correlationMethod     the method to compute feature correlation with (pearson or spearman)
   * @return the computed statistics in a DTO
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   */
  public static StatisticsDTO computeDataFrameStats(
      String name, SparkSession sparkSession, Dataset<Row> sparkDf, String featurestore,
      int version, Boolean descriptiveStatistics, Boolean featureCorrelation,
      Boolean featureHistograms, Boolean clusterAnalysis,
      List<String> statColumns, int numBins, int numClusters,
      String correlationMethod)
    throws DataframeIsEmpty, FeaturestoreNotFound, OnlineFeaturestoreUserNotFound,
    JAXBException, OnlineFeaturestorePasswordNotFound, OnlineFeaturestoreNotEnabled, FeaturegroupDoesNotExistError {
    if (sparkDf == null) {
      sparkDf = getCachedFeaturegroup(sparkSession, name, featurestore, version, false);
    }
    if (statColumns != null && !statColumns.isEmpty()) {
      List<Column> statSparkColumns = statColumns.stream().map(sc -> col(sc)).collect(Collectors.toList());
      Column[] sparkColumnsArr = new Column[statSparkColumns.size()];
      statSparkColumns.toArray(sparkColumnsArr);
      sparkDf = sparkDf.select(sparkColumnsArr);
    }

    if (sparkDf.rdd().isEmpty()) {
      throw new DataframeIsEmpty("The provided dataframe is empty, " +
          "cannot compute feature statistics on an empty dataframe");
    }
    ClusterAnalysisDTO clusterAnalysisDTO = null;
    DescriptiveStatsDTO descriptiveStatsDTO = null;
    FeatureCorrelationMatrixDTO featureCorrelationMatrixDTO = null;
    FeatureDistributionsDTO featureDistributionsDTO = null;

    if (descriptiveStatistics) {
      try {
        LOG.log(Level.INFO, "computing descriptive statistics for: " + name);
        sparkSession.sparkContext().setJobGroup("Descriptive Statistics Computation",
            "Analyzing Dataframe Statistics for : " + name, true);
        descriptiveStatsDTO = computeDescriptiveStatistics(sparkDf);
        sparkSession.sparkContext().setJobGroup("", "", true);
      } catch (Exception e) {
        LOG.log(Level.WARNING, "Could not compute descriptive statistics for:" + name +
            "set the optional argument descriptive_statistics=False to skip this step. Error: " + e.getMessage());
      }
    }

    if (featureCorrelation) {
      try {
        LOG.log(Level.INFO, "computing feature correlation for: " + name);
        sparkSession.sparkContext().setJobGroup("Feature Correlation Computation",
            "Analyzing Feature Correlations for: " + name, true);
        Dataset<Row> numericSparkDf = filterSparkDfNumeric(sparkDf);
        featureCorrelationMatrixDTO = computeCorrMatrix(numericSparkDf, correlationMethod);
        sparkSession.sparkContext().setJobGroup("", "", true);
      } catch (Exception e) {
        LOG.log(Level.WARNING, "Could not compute feature correlation for:" + name +
            "set the optional argument feature_correlation=False to skip this step. Error: " + e.getMessage());
      }
    }

    if (featureHistograms) {
      try {
        LOG.log(Level.INFO, "computing feature histograms for: " + name);
        sparkSession.sparkContext().setJobGroup("Feature Histogram Computation",
            "Analyzing Feature Distributions for: " + name, true);
        Dataset<Row> numericSparkDf = filterSparkDfNumeric(sparkDf);
        featureDistributionsDTO = computeFeatureHistograms(numericSparkDf, numBins);
        sparkSession.sparkContext().setJobGroup("", "", true);
      } catch (Exception e) {
        LOG.log(Level.WARNING, "Could not compute feature histograms for:" + name +
            "set the optional argument feature_histograms=False to skip this step. Error: " + e.getMessage());
      }
    }

    if (clusterAnalysis) {
      try {
        LOG.log(Level.INFO, "computing cluster analysis for: " + name);
        sparkSession.sparkContext().setJobGroup("Feature Cluster Analysis",
            "Analyzing Feature Clusters for: " + name, true);
        Dataset<Row> numericSparkDf = filterSparkDfNumeric(sparkDf);
        clusterAnalysisDTO = computeClusterAnalysis(numericSparkDf, numClusters);
        sparkSession.sparkContext().setJobGroup("", "", true);
      } catch (Exception e) {
        LOG.log(Level.WARNING, "Could not compute cluster analysis for:" + name +
            "set the optional argument cluster_analysis=False to skip this step. Error: " + e.getMessage());
      }
    }
    return new StatisticsDTO(
        descriptiveStatsDTO, clusterAnalysisDTO, featureCorrelationMatrixDTO, featureDistributionsDTO);

  }

  /**
   * Materializes a training dataset dataframe using Spark, writes to HDFS for Hopsworks Training Datasets and to S3
   * for external training datasets
   *
   * @param sparkSession the spark session
   * @param sparkDf      the spark dataframe to materialize
   * @param path     the HDFS path
   * @param dataFormat   the format to serialize to
   * @param writeMode    the spark write mode (append/overwrite)
   * @throws TrainingDatasetFormatNotSupportedError if the provided dataframe format is not supported, supported formats
   * are tfrecords, csv, tsv, avro, orc, image and parquet
   * @throws CannotWriteImageDataFrameException if the user tries to save a dataframe in image format
   */
  public static void writeTrainingDataset(SparkSession sparkSession, Dataset<Row> sparkDf,
                                              String path, String dataFormat, String writeMode) throws
      TrainingDatasetFormatNotSupportedError, CannotWriteImageDataFrameException {
    sparkSession.sparkContext().setJobGroup("Materializing dataframe as training dataset",
        "Saving training dataset in path: " + path + ", in format: " + dataFormat, true);
    switch (dataFormat) {
      case Constants.TRAINING_DATASET_CSV_FORMAT:
        sparkDf.write().option(Constants.SPARK_WRITE_DELIMITER, Constants.COMMA_DELIMITER).mode(
            writeMode).option(Constants.SPARK_WRITE_HEADER, "true").csv(path);
        sparkSession.sparkContext().setJobGroup("", "", true);
        break;
      case Constants.TRAINING_DATASET_TSV_FORMAT:
        sparkDf.write().option(Constants.SPARK_WRITE_DELIMITER, Constants.TAB_DELIMITER)
            .mode(writeMode).option(
            Constants.SPARK_WRITE_HEADER, "true").csv(path);
        sparkSession.sparkContext().setJobGroup("", "", true);
        break;
      case Constants.TRAINING_DATASET_PARQUET_FORMAT:
        sparkDf.write().format(dataFormat).mode(writeMode).parquet(path);
        sparkSession.sparkContext().setJobGroup("", "", true);
        break;
      case Constants.TRAINING_DATASET_AVRO_FORMAT:
        sparkDf.write().format(dataFormat).mode(writeMode).save(path);
        sparkSession.sparkContext().setJobGroup("", "", true);
        break;
      case Constants.TRAINING_DATASET_ORC_FORMAT:
        sparkDf.write().format(dataFormat).mode(writeMode).save(path);
        sparkSession.sparkContext().setJobGroup("", "", true);
        break;
      case Constants.TRAINING_DATASET_IMAGE_FORMAT:
        throw new CannotWriteImageDataFrameException("Image Dataframes can only be read, not written");
      case Constants.TRAINING_DATASET_TFRECORDS_FORMAT:
        sparkDf.write().format(dataFormat).option(Constants.SPARK_TF_CONNECTOR_RECORD_TYPE,
            Constants.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).mode(writeMode).save(path);
        sparkSession.sparkContext().setJobGroup("", "", true);
        break;
      default:
        sparkSession.sparkContext().setJobGroup("", "", true);
        throw new TrainingDatasetFormatNotSupportedError("The provided data format: " + dataFormat +
            " is not supported in the Java/Scala API, the supported data formats are: " +
            Constants.TRAINING_DATASET_CSV_FORMAT + "," +
            Constants.TRAINING_DATASET_TSV_FORMAT + "," +
            Constants.TRAINING_DATASET_TFRECORDS_FORMAT + "," +
            Constants.TRAINING_DATASET_PARQUET_FORMAT + "," +
            Constants.TRAINING_DATASET_AVRO_FORMAT + "," +
            Constants.TRAINING_DATASET_ORC_FORMAT + "," +
            Constants.TRAINING_DATASET_IMAGE_FORMAT + ","
        );
    }
  }

  /**
   * Finds a given feature group from the featurestore metadata by looking for name and version
   *
   * @param trainingDatasetDTOList a list of training datasets for the featurestore
   * @param trainingDatasetName    name of the training dataset to search for
   * @param trainingDatasetVersion version of the training dataset to search for
   * @return the trainingdatasetDTO with the correct name and version
   * @throws TrainingDatasetDoesNotExistError if the name and version combination of the training dataset cannot be
   * resolved.
   */
  public static TrainingDatasetDTO findTrainingDataset(
      List<TrainingDatasetDTO> trainingDatasetDTOList, String trainingDatasetName, int trainingDatasetVersion)
      throws TrainingDatasetDoesNotExistError {
    List<TrainingDatasetDTO> matches = trainingDatasetDTOList.stream().filter(td ->
        td.getName().equals(trainingDatasetName) && td.getVersion() == trainingDatasetVersion)
        .collect(Collectors.toList());
    if (matches.isEmpty()) {
      List<String> trainingDatasetNames =
          trainingDatasetDTOList.stream().map(td -> td.getName()).collect(Collectors.toList());
      throw new TrainingDatasetDoesNotExistError("Could not find the requested training dataset with name: " +
          trainingDatasetName + " , and version: " + trainingDatasetVersion + " , " +
          "among the list of available training datasets in the featurestore: " +
          StringUtils.join(trainingDatasetNames, ","));
    }
    return matches.get(0);
  }


  /**
   * Finds a storage connector with a given name
   *
   * @param storageConnectorsList a list of all storage connector DTOs
   * @param storageConnectorName the name of the storage connector
   * @return the DTO of the storage connector
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   */
  public static FeaturestoreStorageConnectorDTO findStorageConnector(
      List<FeaturestoreStorageConnectorDTO> storageConnectorsList, String storageConnectorName)
      throws StorageConnectorDoesNotExistError {
    List<FeaturestoreStorageConnectorDTO> matches = storageConnectorsList.stream().filter(sc ->
        sc.getName().equals(storageConnectorName)).collect(Collectors.toList());
    if (matches.isEmpty()) {
      List<String> storageConnectorNames =
          storageConnectorsList.stream().map(sc -> sc.getName()).collect(Collectors.toList());
      throw new StorageConnectorDoesNotExistError("Could not find the requested storage connector with name: " +
          storageConnectorName +
          ", among the list of available storage connectors in the featurestore: " +
          StringUtils.join(storageConnectorNames, ","));
    }
    return matches.get(0);
  }


  /**
   * Finds a given training dataset from the featurestore metadata by looking for name and version
   *
   * @param featuregroupDTOList list of feature groups for the featurestore
   * @param featuregroupName    name of the feature group to search for
   * @param featuregroupVersion version of the feature group to search for
   * @return the featuregroupDTO with the correct name and version
   * @throws FeaturegroupDoesNotExistError of the name and version combination of the feature group cannot be resolved.
   */
  public static FeaturegroupDTO findFeaturegroup(
      List<FeaturegroupDTO> featuregroupDTOList, String featuregroupName, int featuregroupVersion)
      throws FeaturegroupDoesNotExistError {
    List<FeaturegroupDTO> matches = featuregroupDTOList.stream().filter(td ->
        td.getName().equals(featuregroupName) && td.getVersion() == featuregroupVersion)
        .collect(Collectors.toList());
    if (matches.isEmpty()) {
      List<String> featuregroupNames =
          featuregroupDTOList.stream().map(td -> td.getName()).collect(Collectors.toList());
      throw new FeaturegroupDoesNotExistError("Could not find the requested feature group with name: " +
          featuregroupName + " , and version: " + featuregroupVersion + " , " +
          "among the list of available feature groups in the featurestore: " +
          StringUtils.join(featuregroupNames, ","));
    }
    return matches.get(0);
  }

  /**
   * Writes the tf records schema for a training dataset to HDFS
   *
   * @param hdfsPath the path to write to
   * @param tfRecordSchemaJson the JSON schema to write
   * @throws IOException IOException
   */
  public static void writeTfRecordSchemaJson(String hdfsPath, String tfRecordSchemaJson) throws IOException {
    Configuration hdfsConf = new Configuration();
    Path filePath = new org.apache.hadoop.fs.Path(hdfsPath);
    FileSystem hdfs = filePath.getFileSystem(hdfsConf);
    try (FSDataOutputStream outputStream = hdfs.create(filePath, true)) {
      outputStream.writeBytes(tfRecordSchemaJson);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Could not save tf record schema json to HDFS", e);
    }
  }

  /**
   * Gets the TFRecords schema in JSON format for a spark dataframe
   *
   * @param sparkDf dataframe to infer tfrecord schema for
   * @return the TFRecords schema as a JSONObject
   * @throws InferTFRecordSchemaError InferTFRecordSchemaError
   */
  public static JSONObject getDataframeTfRecordSchemaJson(Dataset<Row> sparkDf) throws InferTFRecordSchemaError {
    JSONObject tfRecordJsonSchema = new JSONObject();
    for (int i = 0; i < sparkDf.schema().fields().length; i++) {
      if (sparkDf.schema().fields()[i].dataType() instanceof IntegerType) {
        JSONObject featureType = new JSONObject();
        featureType.put(Constants.TF_RECORD_SCHEMA_FEATURE, Constants.TF_RECORD_SCHEMA_FEATURE_FIXED);
        featureType.put(Constants.TF_RECORD_SCHEMA_TYPE, Constants.TF_RECORD_INT_TYPE);
        tfRecordJsonSchema.put(sparkDf.schema().fields()[i].name(), featureType);
      }
      if (sparkDf.schema().fields()[i].dataType() instanceof LongType) {
        JSONObject featureType = new JSONObject();
        featureType.put(Constants.TF_RECORD_SCHEMA_FEATURE, Constants.TF_RECORD_SCHEMA_FEATURE_FIXED);
        featureType.put(Constants.TF_RECORD_SCHEMA_TYPE, Constants.TF_RECORD_INT_TYPE);
        tfRecordJsonSchema.put(sparkDf.schema().fields()[i].name(), featureType);
      }
      if (sparkDf.schema().fields()[i].dataType() instanceof FloatType) {
        JSONObject featureType = new JSONObject();
        featureType.put(Constants.TF_RECORD_SCHEMA_FEATURE, Constants.TF_RECORD_SCHEMA_FEATURE_FIXED);
        featureType.put(Constants.TF_RECORD_SCHEMA_TYPE, Constants.TF_RECORD_FLOAT_TYPE);
        tfRecordJsonSchema.put(sparkDf.schema().fields()[i].name(), featureType);
      }
      if (sparkDf.schema().fields()[i].dataType() instanceof DoubleType) {
        JSONObject featureType = new JSONObject();
        featureType.put(Constants.TF_RECORD_SCHEMA_FEATURE, Constants.TF_RECORD_SCHEMA_FEATURE_FIXED);
        featureType.put(Constants.TF_RECORD_SCHEMA_TYPE, Constants.TF_RECORD_FLOAT_TYPE);
        tfRecordJsonSchema.put(sparkDf.schema().fields()[i].name(), featureType);
      }
      if (sparkDf.schema().fields()[i].dataType() instanceof DecimalType) {
        JSONObject featureType = new JSONObject();
        featureType.put(Constants.TF_RECORD_SCHEMA_FEATURE, Constants.TF_RECORD_SCHEMA_FEATURE_FIXED);
        featureType.put(Constants.TF_RECORD_SCHEMA_TYPE, Constants.TF_RECORD_FLOAT_TYPE);
        tfRecordJsonSchema.put(sparkDf.schema().fields()[i].name(), featureType);
      }
      if (sparkDf.schema().fields()[i].dataType() instanceof StringType) {
        JSONObject featureType = new JSONObject();
        featureType.put(Constants.TF_RECORD_SCHEMA_FEATURE, Constants.TF_RECORD_SCHEMA_FEATURE_FIXED);
        featureType.put(Constants.TF_RECORD_SCHEMA_TYPE, Constants.TF_RECORD_STRING_TYPE);
        tfRecordJsonSchema.put(sparkDf.schema().fields()[i].name(), featureType);
      }
      if (sparkDf.schema().fields()[i].dataType() instanceof BinaryType) {
        JSONObject featureType = new JSONObject();
        featureType.put(Constants.TF_RECORD_SCHEMA_FEATURE, Constants.TF_RECORD_SCHEMA_FEATURE_FIXED);
        featureType.put(Constants.TF_RECORD_SCHEMA_TYPE, Constants.TF_RECORD_STRING_TYPE);
        tfRecordJsonSchema.put(sparkDf.schema().fields()[i].name(), featureType);
      }
      if (sparkDf.schema().fields()[i].dataType() instanceof ArrayType) {
        Row first = sparkDf.first();
        if(first.schema().fields().length > 1) {
          throw new InferTFRecordSchemaError("Cannot Infer TF-Record Schema for spark dataframes with more than one " +
            "nested levels");
        }
        if (first.schema().fields()[i].dataType() instanceof IntegerType){
          JSONObject featureType = new JSONObject();
          featureType.put(Constants.TF_RECORD_SCHEMA_FEATURE, Constants.TF_RECORD_SCHEMA_FEATURE_VAR);
          featureType.put(Constants.TF_RECORD_SCHEMA_TYPE, Constants.TF_RECORD_INT_TYPE);
          tfRecordJsonSchema.put(sparkDf.schema().fields()[i].name(), featureType);
        }
        if (first.schema().fields()[i].dataType() instanceof LongType){
          JSONObject featureType = new JSONObject();
          featureType.put(Constants.TF_RECORD_SCHEMA_FEATURE, Constants.TF_RECORD_SCHEMA_FEATURE_VAR);
          featureType.put(Constants.TF_RECORD_SCHEMA_TYPE, Constants.TF_RECORD_INT_TYPE);
          tfRecordJsonSchema.put(sparkDf.schema().fields()[i].name(), featureType);
        }
        if (first.schema().fields()[i].dataType() instanceof FloatType){
          JSONObject featureType = new JSONObject();
          featureType.put(Constants.TF_RECORD_SCHEMA_FEATURE, Constants.TF_RECORD_SCHEMA_FEATURE_VAR);
          featureType.put(Constants.TF_RECORD_SCHEMA_TYPE, Constants.TF_RECORD_FLOAT_TYPE);
          tfRecordJsonSchema.put(sparkDf.schema().fields()[i].name(), featureType);
        }
        if (first.schema().fields()[i].dataType() instanceof DoubleType){
          JSONObject featureType = new JSONObject();
          featureType.put(Constants.TF_RECORD_SCHEMA_FEATURE, Constants.TF_RECORD_SCHEMA_FEATURE_VAR);
          featureType.put(Constants.TF_RECORD_SCHEMA_TYPE, Constants.TF_RECORD_FLOAT_TYPE);
          tfRecordJsonSchema.put(sparkDf.schema().fields()[i].name(), featureType);
        }
        if (first.schema().fields()[i].dataType() instanceof DecimalType){
          JSONObject featureType = new JSONObject();
          featureType.put(Constants.TF_RECORD_SCHEMA_FEATURE, Constants.TF_RECORD_SCHEMA_FEATURE_VAR);
          featureType.put(Constants.TF_RECORD_SCHEMA_TYPE, Constants.TF_RECORD_FLOAT_TYPE);
          tfRecordJsonSchema.put(sparkDf.schema().fields()[i].name(), featureType);
        }
        if (first.schema().fields()[i].dataType() instanceof StringType){
          JSONObject featureType = new JSONObject();
          featureType.put(Constants.TF_RECORD_SCHEMA_FEATURE, Constants.TF_RECORD_SCHEMA_FEATURE_VAR);
          featureType.put(Constants.TF_RECORD_SCHEMA_TYPE, Constants.TF_RECORD_STRING_TYPE);
          tfRecordJsonSchema.put(sparkDf.schema().fields()[i].name(), featureType);
        }
        if (first.schema().fields()[i].dataType() instanceof BinaryType){
          JSONObject featureType = new JSONObject();
          featureType.put(Constants.TF_RECORD_SCHEMA_FEATURE, Constants.TF_RECORD_SCHEMA_FEATURE_VAR);
          featureType.put(Constants.TF_RECORD_SCHEMA_TYPE, Constants.TF_RECORD_STRING_TYPE);
          tfRecordJsonSchema.put(sparkDf.schema().fields()[i].name(), featureType);
        }
        if (first.schema().fields()[i].dataType() instanceof ArrayType){
          throw new InferTFRecordSchemaError("Can only infer tf record schema for dataframes with one level of nested" +
            " arrays, this dataframe has two levels.");
        }
        if(!(first.schema().fields()[i].dataType() instanceof IntegerType) &&
          !(first.schema().fields()[i].dataType() instanceof LongType) &&
          !(first.schema().fields()[i].dataType() instanceof FloatType) &&
          !(first.schema().fields()[i].dataType() instanceof DoubleType) &&
          !(first.schema().fields()[i].dataType() instanceof DecimalType) &&
          !(first.schema().fields()[i].dataType() instanceof StringType) &&
          !(first.schema().fields()[i].dataType() instanceof BinaryType)) {
          throw new InferTFRecordSchemaError("Could not infer the tf record schema, an array column has the datatype:" +
            " " +
            first.schema().fields()[i].dataType().toString() + " which is not in the list of recognized types: " +
            " IntegerType, LongType, FloatType, DoubleType, DecimalType, StringType, and BinaryType");
        }
      }
      if(!(sparkDf.schema().fields()[i].dataType() instanceof IntegerType) &&
        !(sparkDf.schema().fields()[i].dataType() instanceof LongType) &&
        !(sparkDf.schema().fields()[i].dataType() instanceof FloatType) &&
        !(sparkDf.schema().fields()[i].dataType() instanceof DoubleType) &&
        !(sparkDf.schema().fields()[i].dataType() instanceof DecimalType) &&
        !(sparkDf.schema().fields()[i].dataType() instanceof StringType) &&
        !(sparkDf.schema().fields()[i].dataType() instanceof BinaryType) &&
        !(sparkDf.schema().fields()[i].dataType() instanceof ArrayType)
      ){
        throw new InferTFRecordSchemaError("Could not infer the tf record schema, a row has the datatype: " +
          sparkDf.schema().fields()[i].dataType().toString() + " which is not in the list of recognized types: " +
          " IntegerType, LongType, FloatType, DoubleType, DecimalType, StringType, BinaryType, and ArrayType");
      }
    }
    LOG.log(Level.INFO, "Inferred TFRecordsSchema: " + tfRecordJsonSchema.toString());
    return tfRecordJsonSchema;
  }

  /**
   * Gets the latest version of a feature group, returns 0 if no version exists
   *
   * @param featuregroupDTOS the list of feature groups to search through
   * @param featuregroupName the name of the feature group to get the latest version of
   * @return the latest version of the training dataset, 0 if no version exists
   */
  public static int getLatestFeaturegroupVersion(List<FeaturegroupDTO> featuregroupDTOS, String featuregroupName) {
    List<FeaturegroupDTO> matches = featuregroupDTOS.stream().
        filter(fg -> fg.getName().equals(featuregroupName)).collect(Collectors.toList());
    if (matches.isEmpty())
      return 0;
    else {
      return Collections.max(matches.stream().map(fg -> fg.getVersion()).collect(Collectors.toList()));
    }
  }

  /**
   * Gets the latest version of a training dataset, returns 0 if no version exists
   *
   * @param trainingDatsetDTOS  the list of training datasets to search through
   * @param trainingDatasetName the name of the training dataset to get the latest version of
   * @return the latest version of the training dataset, 0 if no version exists
   */
  public static int getLatestTrainingDatasetVersion(
      List<TrainingDatasetDTO> trainingDatsetDTOS,
      String trainingDatasetName) {
    List<TrainingDatasetDTO> matches = trainingDatsetDTOS.stream().
        filter(td -> td.getName().equals(trainingDatasetName)).collect(Collectors.toList());
    if (matches.isEmpty())
      return 0;
    else {
      return Collections.max(matches.stream().map(fg -> fg.getVersion()).collect(Collectors.toList()));
    }
  }

  /**
   * Get the cached metadata of the feature store
   *
   * @return the feature store metadata cache
   */
  public static FeaturestoreMetadataDTO getFeaturestoreMetadataCache() {
    return featurestoreMetadataCache;
  }

  /**
   * Update cache of the feature store metadata
   *
   * @param featurestoreMetadataCache the new value of the cache
   */
  public static void setFeaturestoreMetadataCache(
    FeaturestoreMetadataDTO featurestoreMetadataCache) {
    FeaturestoreHelper.featurestoreMetadataCache = featurestoreMetadataCache;
  }

  /**
   * If featurestore is specififed return it, otherwise return default value
   *
   * @param featurestore featurestore
   * @return featurestore or default value
   */
  public static String featurestoreGetOrDefault(String featurestore){
    if (featurestore == null)
      return FeaturestoreHelper.getProjectFeaturestore();
    return featurestore;
  }

  /**
   * If sparkSession is specififed return it, otherwise return default value
   * @param sparkSession sparkSession
   * @return sparkSession or default value
   */
  public static SparkSession sparkGetOrDefault(SparkSession sparkSession){
    if (sparkSession == null)
      return Hops.findSpark();
    return sparkSession;
  }

  /**
   * If primaryKey is specififed return it, otherwise return default value
   *
   * @param primaryKeys the primary keys
   * @param df dataframe
   * @return primaryKey or default value
   */
  public static List<String> primaryKeyGetOrDefault(List<String> primaryKeys, Dataset<Row> df){
    if (primaryKeys.isEmpty()) {
      return FeaturestoreHelper.getDefaultPrimaryKeys(df);
    }
    return primaryKeys;
  }

  /**
   * If corrMethod is specififed return it, otherwise return default value
   * @param corrMethod corrMethod
   * @return corrMethod or default value
   */
  public static String correlationMethodGetOrDefault(String corrMethod){
    if (corrMethod == null) {
      return Constants.CORRELATION_ANALYSIS_DEFAULT_METHOD;
    }
    return corrMethod;
  }

  /**
   * If numBins is specififed return it, otherwise return default value
   * @param numBins numBins
   * @return numBins or default value
   */
  public static Integer numBinsGetOrDefault(Integer numBins){
    if (numBins == null) {
      return  20;
    }
    return numBins;
  }

  /**
   * If numClusters is specififed return it, otherwise return default value
   * @param numClusters numClusters
   * @return numClusters or default value
   */
  public static Integer numClustersGetOrDefault(Integer numClusters){
    if (numClusters == null) {
      return 5;
    }
    return numClusters;
  }

  /**
   * If jobName is specififed return it, otherwise return default value
   * @param jobName jobName
   * @return jobName or default value
   */
  public static String jobNameGetOrDefault(String jobName){
    if(Strings.isNullOrEmpty(jobName)){
      return Hops.getJobName();
    }
    return jobName;
  }

  /**
   * If dataFormat is specififed return it, otherwise return default value
   * @param dataFormat dataFormat
   * @return dataFormat or default value
   */
  public static String dataFormatGetOrDefault(String dataFormat){
    if (dataFormat == null) {
      return Constants.TRAINING_DATASET_TFRECORDS_FORMAT;
    }
    return dataFormat;
  }

  /**
   * Gets the id of a featurestore (temporary workaround until HOPSWORKS-860 where we use Name as unique identifier for
   * resources)
   *
   * @param featurestore name of the featurestore to get the id of
   * @return the id of the featurestore (primary key in hopsworks db featurestore table)
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   */
  public static Integer getFeaturestoreId(String featurestore) throws JAXBException, FeaturestoreNotFound {
    if(getFeaturestoreMetadataCache() == null ||
      !featurestore.equalsIgnoreCase(getFeaturestoreMetadataCache().getFeaturestore().getFeaturestoreName()))
      new FeaturestoreUpdateMetadataCache().setFeaturestore(featurestore).write();
    return getFeaturestoreMetadataCache().getFeaturestore().getFeaturestoreId();
  }

  /**
   * Gets the id of a featuregroup (temporary workaround until HOPSWORKs-860) where we use Name as a unique identifier
   * for resources)
   *
   * @param featurestore the featurestore where the featuregroup belongs
   * @param featuregroup the featuregroup to get the id of
   * @param featuregroupVersion the version of the featuregroup
   * @return the id of the featuregroup
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   */
  public static Integer getFeaturegroupId(String featurestore, String featuregroup, int featuregroupVersion)
    throws JAXBException, FeaturestoreNotFound, FeaturegroupDoesNotExistError {
    if(getFeaturestoreMetadataCache() == null ||
      !featurestore.equalsIgnoreCase(getFeaturestoreMetadataCache().getFeaturestore().getFeaturestoreName()))
      new FeaturestoreUpdateMetadataCache().setFeaturestore(featurestore).write();
    List<FeaturegroupDTO> featuregroups =
      getFeaturestoreMetadataCache().getFeaturegroups()
        .stream().filter(fg ->
        fg.getName().equalsIgnoreCase(featuregroup) &&
          fg.getVersion() == featuregroupVersion).collect(Collectors.toList());
    if(featuregroups.size() == 1){
      return featuregroups.get(0).getId();
    }
    if(featuregroups.size() > 1){
      throw new AssertionError(" Found more than one featuregroup with the name: " + featuregroup + " in the " +
        "featurestore: " + featurestore);
    }
    throw new FeaturegroupDoesNotExistError("Featuregroup: " + featuregroup + " was not found in the featurestore: "
        + featurestore);
  }

  /**
   * Gets the id of a training dataset (temporary workaround until HOPSWORKS-860) where we use Name as a unique
   * identifier for resources)
   *
   * @param featurestore the featurestore where the training dataset belongs
   * @param trainingDataset the training dataset to get the id of
   * @param trainingDatasetVersion the version of the training dataset
   * @return the id of the training dataset
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   */
  public static Integer getTrainingDatasetId(String featurestore, String trainingDataset, int trainingDatasetVersion)
    throws JAXBException, FeaturestoreNotFound, TrainingDatasetDoesNotExistError {
    if(getFeaturestoreMetadataCache() == null ||
      !featurestore.equalsIgnoreCase(getFeaturestoreMetadataCache().getFeaturestore().getFeaturestoreName()))
      new FeaturestoreUpdateMetadataCache().setFeaturestore(featurestore).write();
    List<TrainingDatasetDTO> trainingDatasets =
      getFeaturestoreMetadataCache().getTrainingDatasets()
        .stream().filter(td ->
        td.getName().equalsIgnoreCase(trainingDataset) &&
        td.getVersion() == trainingDatasetVersion).collect(Collectors.toList());
    if(trainingDatasets.size() == 1){
      return trainingDatasets.get(0).getId();
    }
    if(trainingDatasets.size() > 1){
      throw new AssertionError(" Found more than one training dataset with the name: " + trainingDataset + " in the " +
        "featurestore: " + featurestore);
    }
    throw new TrainingDatasetDoesNotExistError("Training Dataset: " + trainingDataset + " was not found in the " +
      "featurestore: "
      + featurestore);
  }

  /**
   * Checks whether Hive is enabled for a given spark session
   *
   * @param sparkSession the spark session
   * @return true if Hive is enabled, otherwise false
   */
  public static Boolean isHiveEnabled(SparkSession sparkSession) {
    return getSparkSqlCatalogImpl(sparkSession).equalsIgnoreCase(Constants.SPARK_SQL_CATALOG_HIVE);
  }

  /**
   * Gets the SparkSQL catalog implementation for a spark session
   *
   * @param sparkSession the session to get the catalog implementation for
   * @return the SparkSQL catalog implementation of the spark session
   */
  public static String getSparkSqlCatalogImpl(SparkSession sparkSession) {
    return sparkSession.sparkContext().getConf().get(Constants.SPARK_SQL_CATALOG_IMPLEMENTATION);
  }

  /**
   * Verify that hive is enabled on the spark session. If Hive is not enabled, the featurestore API will not work
   * as it depends on SparkSQL backed by Hive tables
   *
   * @param sparkSession the sparkSession to verify
   * @throws HiveNotEnabled thrown if Hive is not enabled
   */
  public static void verifyHiveEnabled(SparkSession sparkSession) throws HiveNotEnabled {
    if(!isHiveEnabled(sparkSession)){
      throw new HiveNotEnabled("Hopsworks Featurestore Depends on Hive. Hive is not enabled " +
        "for the current spark session. " +
        "Make sure to enable hive before using the featurestore API." +
        " The current SparkSQL catalog implementation is: " + getSparkSqlCatalogImpl(sparkSession) +
        ", it should be: " +  Constants.SPARK_SQL_CATALOG_HIVE);
    }
  }

  /**
   * Returns the DTO type for a training dataset
   *
   * @param trainingDatasetDTO the training dataset
   * @param featurestoreClientSettingsDTO the client settings
   * @return the DTO type of the training dataset
   */
  public static String getTrainingDatasetDTOTypeStr(TrainingDatasetDTO trainingDatasetDTO,
                                                    FeaturestoreClientSettingsDTO featurestoreClientSettingsDTO) {
    if(trainingDatasetDTO.getTrainingDatasetType() == TrainingDatasetType.HOPSFS_TRAINING_DATASET){
      return featurestoreClientSettingsDTO.getHopsfsTrainingDatasetDtoType();
    } else {
      return featurestoreClientSettingsDTO.getExternalTrainingDatasetDtoType();
    }
  }

  /**
   * Gets the project's default location for storing training datasets in HopsFS
   *
   * @return the name of the default storage connector for storing training datasets in HopsFS
   */
  public static String getProjectTrainingDatasetsSink() {
    String projectName = Hops.getProjectName();
    return projectName + Constants.TRAINING_DATASETS_SUFFIX;
  }

  /**
   * Returns the feature group DTO type
   *
   * @param featurestoreClientSettingsDTO the settings DTO
   * @param onDemand boolean flag whether it is an on-demand feature group
   * @return the DTO string type
   */
  public static String getFeaturegroupDtoTypeStr(FeaturestoreClientSettingsDTO featurestoreClientSettingsDTO,
    Boolean onDemand) {
    if(onDemand) {
      return featurestoreClientSettingsDTO.getOnDemandFeaturegroupDtoType();
    } else {
      return featurestoreClientSettingsDTO.getCachedFeaturegroupDtoType();
    }
  }

  /**
   * Returns the feature group type string
   *
   * @param onDemand boolean flag whether it is an on-demand feature group or not
   * @return the feature group type string
   */
  public static String getFeaturegroupTypeStr(Boolean onDemand) {
    if(onDemand) {
      return FeaturegroupType.ON_DEMAND_FEATURE_GROUP.name();
    } else {
      return FeaturegroupType.CACHED_FEATURE_GROUP.name();
    }
  }

  /**
   * Gets the path to where a hopsfs training dataset is
   *
   * @param hopsfsTrainingDatasetDTO information about the training dataset
   * @return the HDFS path
   */
  public static String getHopsfsTrainingDatasetPath(HopsfsTrainingDatasetDTO hopsfsTrainingDatasetDTO) {
    return Constants.HDFS_DEFAULT + hopsfsTrainingDatasetDTO.getHdfsStorePath() +
      Constants.SLASH_DELIMITER + hopsfsTrainingDatasetDTO.getName();
  }

  /**
   * Verify user-provided dataframe
   *
   * @param dataframe the dataframe to validate
   */
  public static void validateDataframe(Dataset<Row> dataframe) {
    if(dataframe == null){
      throw new IllegalArgumentException("Dataframe cannot be null, specify dataframe with " +
        ".setDataframe(df)");
    }
  }

  /**
   * Validate user-provided write-mode parameter
   *
   * @param mode the mode to verify
   */
  public static void validateWriteMode(String mode) {
    if (mode==null || !mode.equalsIgnoreCase("append") && !mode.equalsIgnoreCase("overwrite"))
      throw new IllegalArgumentException("The supplied write mode: " + mode +
        " does not match any of the supported modes: overwrite, append");
  }

  /**
   * Registers custom JDBC dialects for the Feature Store
   */
  public static void registerCustomJdbcDialects(){
    JdbcDialects.registerDialect(getHiveJdbcDialect());
  }

  /**
   * Custom JDBC dialect for reading from Hive with JDBC. This dialect can be registered with Spark to enable reading
   * from Hive using JDBC rather than SparkSQL.
   *
   * @return the custom JDBC dialect
   */
  public static JdbcDialect getHiveJdbcDialect() {
    JdbcDialect hiveDialect = new JdbcDialect() {
      @Override
      public boolean canHandle(String url) {
        return url.startsWith("jdbc:hive2") || url.contains("hive2");
      }

      @Override
      public String quoteIdentifier(String colName) {
        return colName;
      }

    };
    return hiveDialect;
  }

  /**
   * Converts a java string list to a scala sequence
   *
   * @param inputList the java list to convert
   * @return scala seq
   */
  public static Seq<String> convertListToSeq(List<String> inputList) {
    return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
  }

  /**
   * Registers a list of on-demand featuregroups as temporary tables in SparkSQL. First fetches the on-demand
   * featuregroups using JDBC and the provided SQL string and then registers the resulting spark dataframes as
   * temporary tables with the name featuregroupname_featuregroupversion
   *
   * @param onDemandFeaturegroups the list of on demand feature groups
   * @param featurestore the featurestore to query
   * @param jdbcArguments jdbc arguments for fetching the on-demand featuregroups
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws HiveNotEnabled HiveNotEnabled
   * @throws StorageConnectorDoesNotExistError StorageConnectorDoesNotExistError
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws OnlineFeaturestoreNotEnabled OnlineFeaturestoreNotEnabled
   */
  public static void registerOnDemandFeaturegroupsAsTempTables(
      List<FeaturegroupDTO> onDemandFeaturegroups, String featurestore, Map<String, Map<String, String>> jdbcArguments)
    throws FeaturegroupDoesNotExistError, HiveNotEnabled, StorageConnectorDoesNotExistError,
    OnlineFeaturestorePasswordNotFound, FeaturestoreNotFound, OnlineFeaturestoreUserNotFound, JAXBException,
    OnlineFeaturestoreNotEnabled {
    for (FeaturegroupDTO onDemandFeaturegroup : onDemandFeaturegroups) {
      FeaturestoreReadFeaturegroup readFeaturegroupOp = new FeaturestoreReadFeaturegroup(onDemandFeaturegroup.getName())
          .setVersion(onDemandFeaturegroup.getVersion())
          .setFeaturestore(featurestore);
      if(jdbcArguments != null &&
          jdbcArguments.containsKey(getTableName(onDemandFeaturegroup.getName(), onDemandFeaturegroup.getVersion()))) {
        readFeaturegroupOp.setJdbcArguments(
            jdbcArguments.get(getTableName(onDemandFeaturegroup.getName(), onDemandFeaturegroup.getVersion())));
      }
      Dataset<Row> sparkDf = readFeaturegroupOp.read();
      sparkDf.registerTempTable(getTableName(onDemandFeaturegroup.getName(), onDemandFeaturegroup.getVersion()));
      LOG.info("Registered On-Demand Feature Group: " + onDemandFeaturegroup.getName() + " with version: " +
          onDemandFeaturegroup.getVersion() + " as temporary table: " +
          getTableName(onDemandFeaturegroup.getName(), onDemandFeaturegroup.getVersion()));
    }
  }
  
  /**
   * Utility function for getting the S3 path of an external training dataset in the feature store
   *
   * @param trainingDatasetName the name of the training dataset
   * @param trainingDatasetVersion the version of the training dataset
   * @param bucket the S3 bucket
   * @return the path to the training dataset
   */
  public static String getExternalTrainingDatasetPath(String trainingDatasetName, int trainingDatasetVersion,
    String bucket) {
    String path = "";
    if(!path.contains(Constants.S3_FILE_PREFIX)) {
      path = path + Constants.S3_FILE_PREFIX;
    }
    path =
      path + bucket + Constants.SLASH_DELIMITER +  Constants.S3_TRAINING_DATASETS_FOLDER
        + Constants.SLASH_DELIMITER + FeaturestoreHelper.getTableName(trainingDatasetName, trainingDatasetVersion);
    return path;
  }
  
  /**
   * Utility function that registers access key and secret key environment variables for writing/read to/from S3
   * with Spark
   *
   * @param accessKey the s3 access key
   * @param secretKey the s3 secret key
   * @param sparkSession the spark session
   */
  public static void setupS3CredentialsForSpark(String accessKey, String secretKey, SparkSession sparkSession) {
    SparkContext sparkContext = sparkSession.sparkContext();
    sparkContext.hadoopConfiguration().set(Constants.S3_ACCESS_KEY_ENV, accessKey);
    sparkContext.hadoopConfiguration().set(Constants.S3_SECRET_KEY_ENV, secretKey);
  }
  
  /**
   * Utility function for getting the S3 path of a feature dataset on S3
   *
   * @param datasetPath path to the dataset on S3
   * @param bucket the S3 bucket
   * @return S3 path to the dataset (bucket and file prefix appended if not in the user-supplied path
   */
  public static String getBucketPath(String bucket, String datasetPath) {
    if(datasetPath.contains(bucket)){
      if(!datasetPath.contains(Constants.S3_FILE_PREFIX)) {
        return Constants.S3_FILE_PREFIX + datasetPath;
      } else {
        return datasetPath;
      }
    }
    String path = "";
    if(!path.contains(Constants.S3_FILE_PREFIX)) {
      path = path + Constants.S3_FILE_PREFIX;
    }
    path = path + bucket + Constants.SLASH_DELIMITER + datasetPath;
    return path;
  }
  
  /**
   * Extracts the password from a online-featurestore storage connector
   *
   * @param featurestoreJdbcConnectorDTO the connector dto to extract the password from
   * @return the password (string)
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   */
  public static String getOnlineFeaturestorePassword(FeaturestoreJdbcConnectorDTO featurestoreJdbcConnectorDTO)
    throws OnlineFeaturestorePasswordNotFound {
    String[] args = featurestoreJdbcConnectorDTO.getArguments().split(Constants.COMMA_DELIMITER);
    for (int i = 0; i < args.length; i++) {
      if(args[i].contains("password=")){
        return args[i].replace("password=", "");
      }
    }
    throw new OnlineFeaturestorePasswordNotFound("Could not find any password in the storage connector");
  }
  
  /**
   * Extracts the username from a online-featurestore storage connector
   *
   * @param featurestoreJdbcConnectorDTO the connector dto to extract the user from
   * @return the username (string)
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   */
  public static String getOnlineFeaturestoreUser(FeaturestoreJdbcConnectorDTO featurestoreJdbcConnectorDTO)
    throws OnlineFeaturestoreUserNotFound {
    String[] args = featurestoreJdbcConnectorDTO.getArguments().split(Constants.COMMA_DELIMITER);
    for (int i = 0; i < args.length; i++) {
      if(args[i].contains("user=")){
        return args[i].replace("user=", "");
      }
    }
    throw new OnlineFeaturestoreUserNotFound("Could not find any username in the storage connector");
  }
  
  /**
   * Writes a Spark  dataframe to the online feature store using a JDBC conenctor
   *
   * @param sparkDf the dataframe to write
   * @param featurestoreJdbcConnectorDTO the jdbc connector to the online feature store
   * @param tableName the name of the table to write the dataframe to
   * @param mode the spark write mode
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   */
  public static void writeJdbcDataframe(Dataset<Row> sparkDf, FeaturestoreJdbcConnectorDTO featurestoreJdbcConnectorDTO,
    String tableName, String mode)
    throws OnlineFeaturestorePasswordNotFound, OnlineFeaturestoreUserNotFound {
    String pw = getOnlineFeaturestorePassword(featurestoreJdbcConnectorDTO);
    String user = getOnlineFeaturestoreUser(featurestoreJdbcConnectorDTO);
    sparkDf.write().format(Constants.SPARK_JDBC_FORMAT)
      .option(Constants.SPARK_JDBC_URL, featurestoreJdbcConnectorDTO.getConnectionString())
      .option(Constants.SPARK_JDBC_DBTABLE, tableName)
      .option(Constants.SPARK_JDBC_USER, user)
      .option(Constants.SPARK_JDBC_PW, pw)
      .mode(mode)
      .save();
  }
  
  /**
   * Reads a spark dataframe from the online feature store
   *
   * @param sparkSession the spark session
   * @param featurestoreJdbcConnectorDTO  the storage connector to connect to the online featurestore
   * @param query the SQL query for querying the online feature store
   * @return the resulting spark dataframe
   * @throws OnlineFeaturestorePasswordNotFound OnlineFeaturestorePasswordNotFound
   * @throws OnlineFeaturestoreUserNotFound OnlineFeaturestoreUserNotFound
   */
  public static Dataset<Row> readJdbcDataFrame(SparkSession sparkSession,
    FeaturestoreJdbcConnectorDTO featurestoreJdbcConnectorDTO, String query)
    throws OnlineFeaturestorePasswordNotFound, OnlineFeaturestoreUserNotFound {
    String pw = getOnlineFeaturestorePassword(featurestoreJdbcConnectorDTO);
    String user = getOnlineFeaturestoreUser(featurestoreJdbcConnectorDTO);
    return sparkSession.read().format(Constants.SPARK_JDBC_FORMAT)
      .option(Constants.SPARK_JDBC_URL, featurestoreJdbcConnectorDTO.getConnectionString())
      .option(Constants.SPARK_JDBC_DBTABLE, query)
      .option(Constants.SPARK_JDBC_USER, user)
      .option(Constants.SPARK_JDBC_PW, pw)
      .load();
  }
  

}
