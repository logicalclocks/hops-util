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


import io.hops.util.Constants;
import io.hops.util.Hops;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.InvalidPrimaryKeyForFeaturegroup;
import io.hops.util.exceptions.SparkDataTypeNotRecognizedError;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetFormatNotSupportedError;
import io.hops.util.featurestore.feature.FeatureDTO;
import io.hops.util.featurestore.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.stats.StatisticsDTO;
import io.hops.util.featurestore.stats.cluster_analysis.ClusterAnalysisDTO;
import io.hops.util.featurestore.stats.cluster_analysis.ClusterDTO;
import io.hops.util.featurestore.stats.cluster_analysis.DatapointDTO;
import io.hops.util.featurestore.stats.desc_stats.DescriptiveStatsDTO;
import io.hops.util.featurestore.stats.desc_stats.DescriptiveStatsMetricValueDTO;
import io.hops.util.featurestore.stats.desc_stats.DescriptiveStatsMetricValuesDTO;
import io.hops.util.featurestore.stats.feature_correlation.CorrelationValueDTO;
import io.hops.util.featurestore.stats.feature_correlation.FeatureCorrelationDTO;
import io.hops.util.featurestore.stats.feature_correlation.FeatureCorrelationMatrixDTO;
import io.hops.util.featurestore.stats.feature_distributions.FeatureDistributionDTO;
import io.hops.util.featurestore.stats.feature_distributions.FeatureDistributionsDTO;
import io.hops.util.featurestore.stats.feature_distributions.HistogramBinDTO;
import io.hops.util.featurestore.trainingdataset.TrainingDatasetDTO;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
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
  private static JAXBContext descriptiveStatsJAXBContext;
  private static JAXBContext featureCorrelationJAXBContext;
  private static JAXBContext featureHistogramsJAXBContext;
  private static JAXBContext clusterAnalysisJAXBContext;
  private static JAXBContext featureJAXBContext;
  private static JAXBContext featuregroupsAndTrainingDatasetsJAXBContext;
  private static JAXBContext trainingDatasetJAXBContext;

  private static Marshaller descriptiveStatsMarshaller;
  private static Marshaller featureCorrelationMarshaller;
  private static Marshaller featureHistogramsMarshaller;
  private static Marshaller clusteranalysisMarshaller;
  private static Marshaller featureMarshaller;
  private static Marshaller featuregroupsAndTrainingDatasetsMarshaller;
  private static Marshaller trainingDatasetMarshaller;

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
      featuregroupsAndTrainingDatasetsJAXBContext =
          JAXBContextFactory.createContext(new Class[]{FeaturegroupsAndTrainingDatasetsDTO.class}, null);
      trainingDatasetJAXBContext =
          JAXBContextFactory.createContext(new Class[]{TrainingDatasetDTO.class}, null);
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
      featuregroupsAndTrainingDatasetsMarshaller = featuregroupsAndTrainingDatasetsJAXBContext.createMarshaller();
      featuregroupsAndTrainingDatasetsMarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      featuregroupsAndTrainingDatasetsMarshaller.setProperty(
          MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      trainingDatasetMarshaller = trainingDatasetJAXBContext.createMarshaller();
      trainingDatasetMarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      trainingDatasetMarshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
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
    return projectName + "_featurestore";
  }

  /**
   * Selects the featurestore database in SparkSQL
   *
   * @param sparkSession the spark session
   * @param featurestore the featurestore database to select
   */
  public static void useFeaturestore(SparkSession sparkSession, String featurestore) {
    if (featurestore == null)
      featurestore = getProjectFeaturestore();
    String sqlStr = "use " + featurestore;
    logAndRunSQL(sparkSession, sqlStr);
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
   * Saves the given dataframe to the specified featuregroup. Defaults to the project-featurestore
   * This will append to  the featuregroup. To overwrite a featuregroup, create a new version of the featuregroup
   * from the UI and append to that table.
   *
   * @param sparkDf             the dataframe containing the data to insert into the featuregroup
   * @param sparkSession        the spark session
   * @param featuregroup        the name of the featuregroup (hive table name)
   * @param featurestore        the featurestore to save the featuregroup to (hive database)
   * @param featuregroupVersion the version of the featuregroup
   */
  public static void insertIntoFeaturegroup(Dataset<Row> sparkDf, SparkSession sparkSession,
                                            String featuregroup, String featurestore,
                                            int featuregroupVersion) {
    useFeaturestore(sparkSession, featurestore);
    String tableName = getTableName(featuregroup, featuregroupVersion);
    //overwrite is not supported because it will drop the table and create a new one,
    //this means that all the featuregroup metadata will be dropped due to ON DELETE CASCADE
    String mode = "append";
    //Specify format hive as it is managed table
    String format = "hive";
    sparkDf.write().format(format).mode(mode).saveAsTable(tableName);
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
   * Gets a featuregroup from a particular featurestore
   *
   * @param sparkSession        the spark session
   * @param featuregroup        the featuregroup to get
   * @param featurestore        the featurestre to query
   * @param featuregroupVersion the version of the featuregroup to get
   * @return a spark dataframe with the featuregroup
   */
  public static Dataset<Row> getFeaturegroup(SparkSession sparkSession, String featuregroup,
                                             String featurestore, int featuregroupVersion) {
    useFeaturestore(sparkSession, featurestore);
    String sqlStr = "SELECT * FROM " + getTableName(featuregroup, featuregroupVersion);
    return logAndRunSQL(sparkSession, sqlStr);
  }

  /**
   * Gets a training dataset from a feature store
   *
   * @param sparkSession the spark session
   * @param dataFormat   the data format of the training dataset
   * @param hdfsPath     the hdfs path to the dataset
   * @return a spark dataframe with the dataset
   * @throws TrainingDatasetFormatNotSupportedError if a unsupported data format is provided, supported modes are:
   * tfrecords, tsv, csv, and parquet
   * @throws IOException IOException IOException
   * @throws TrainingDatasetDoesNotExistError if the hdfsPath is not found
   */
  public static Dataset<Row> getTrainingDataset(SparkSession sparkSession, String dataFormat, String hdfsPath)
      throws TrainingDatasetFormatNotSupportedError, IOException, TrainingDatasetDoesNotExistError {
    Configuration hdfsConf = new Configuration();
    Path filePath = null;
    FileSystem hdfs = null;
    switch (dataFormat) {
      case Constants.TRAINING_DATASET_CSV_FORMAT:
        filePath = new org.apache.hadoop.fs.Path(hdfsPath);
        hdfs = filePath.getFileSystem(hdfsConf);
        if (hdfs.exists(filePath)) {
          return sparkSession.read().format(dataFormat).option(Constants.SPARK_WRITE_HEADER, "true")
              .option(Constants.SPARK_WRITE_DELIMITER, Constants.COMMA_DELIMITER).load(hdfsPath);
        } else {
          filePath = new org.apache.hadoop.fs.Path(hdfsPath + Constants.TRAINING_DATASET_CSV_SUFFIX);
          hdfs = filePath.getFileSystem(hdfsConf);
          if (hdfs.exists(filePath)) {
            return sparkSession.read().format(dataFormat).option(Constants.SPARK_WRITE_HEADER, "true")
                .option(Constants.SPARK_WRITE_DELIMITER, Constants.COMMA_DELIMITER).load(hdfsPath +
                    Constants.TRAINING_DATASET_CSV_SUFFIX);
          } else {
            throw new TrainingDatasetDoesNotExistError("Could not find any training dataset in folder : "
                + hdfsPath + " or in file: " + hdfsPath + Constants.TRAINING_DATASET_CSV_SUFFIX);
          }
        }
      case Constants.TRAINING_DATASET_TSV_FORMAT:
        filePath = new org.apache.hadoop.fs.Path(hdfsPath);
        hdfs = filePath.getFileSystem(hdfsConf);
        if (hdfs.exists(filePath)) {
          return sparkSession.read().format(dataFormat).option(Constants.SPARK_WRITE_HEADER, "true")
              .option(Constants.SPARK_WRITE_DELIMITER, Constants.TAB_DELIMITER).load(hdfsPath);
        } else {
          filePath = new org.apache.hadoop.fs.Path(hdfsPath + Constants.TRAINING_DATASET_TSV_SUFFIX);
          hdfs = filePath.getFileSystem(hdfsConf);
          if (hdfs.exists(filePath)) {
            return sparkSession.read().format(dataFormat).option(Constants.SPARK_WRITE_HEADER, "true")
                .option(Constants.SPARK_WRITE_DELIMITER, Constants.TAB_DELIMITER).load(hdfsPath +
                    Constants.TRAINING_DATASET_TSV_SUFFIX);
          } else {
            throw new TrainingDatasetDoesNotExistError("Could not find any training dataset in folder : "
                + hdfsPath + " or in file: " + hdfsPath + Constants.TRAINING_DATASET_TSV_SUFFIX);
          }
        }
      case Constants.TRAINING_DATASET_PARQUET_FORMAT:
        filePath = new org.apache.hadoop.fs.Path(hdfsPath);
        hdfs = filePath.getFileSystem(hdfsConf);
        if (hdfs.exists(filePath)) {
          return sparkSession.read().parquet(hdfsPath);
        } else {
          filePath = new org.apache.hadoop.fs.Path(hdfsPath + Constants.TRAINING_DATASET_PARQUET_SUFFIX);
          hdfs = filePath.getFileSystem(hdfsConf);
          if (hdfs.exists(filePath)) {
            return sparkSession.read().parquet(hdfsPath + Constants.TRAINING_DATASET_PARQUET_SUFFIX);
          } else {
            throw new TrainingDatasetDoesNotExistError("Could not find any training dataset in folder : "
                + hdfsPath + " or in file: " + hdfsPath + Constants.TRAINING_DATASET_PARQUET_SUFFIX);
          }
        }
      case Constants.TRAINING_DATASET_TFRECORDS_FORMAT:
        filePath = new org.apache.hadoop.fs.Path(hdfsPath);
        hdfs = filePath.getFileSystem(hdfsConf);
        if (hdfs.exists(filePath)) {
          return sparkSession.read().format(dataFormat).option(Constants.SPARK_TF_CONNECTOR_RECORD_TYPE,
              Constants.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(hdfsPath);
        } else {
          filePath = new org.apache.hadoop.fs.Path(hdfsPath + Constants.TRAINING_DATASET_TFRECORDS_SUFFIX);
          hdfs = filePath.getFileSystem(hdfsConf);
          if (hdfs.exists(filePath)) {
            return sparkSession.read().format(dataFormat).option(Constants.SPARK_TF_CONNECTOR_RECORD_TYPE,
                Constants.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).load(hdfsPath +
                Constants.TRAINING_DATASET_TFRECORDS_SUFFIX);
          } else {
            throw new TrainingDatasetDoesNotExistError("Could not find any training dataset in folder : "
                + hdfsPath + " or in file: " + hdfsPath + Constants.TRAINING_DATASET_TFRECORDS_SUFFIX);
          }
        }
      default:
        throw new TrainingDatasetFormatNotSupportedError("The provided data format: " + dataFormat +
            " is not supported in the Java/Scala API, the supported data formats are: " +
            Constants.TRAINING_DATASET_CSV_FORMAT + "," +
            Constants.TRAINING_DATASET_TSV_FORMAT + "," +
            Constants.TRAINING_DATASET_TFRECORDS_FORMAT + "," +
            Constants.TRAINING_DATASET_PARQUET_FORMAT + ",");
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
   * @return A dataframe with the feature
   */
  public static Dataset<Row> getFeature(
      SparkSession sparkSession, String feature,
      String featurestore, List<FeaturegroupDTO> featuregroupDTOS) {
    useFeaturestore(sparkSession, featurestore);
    FeaturegroupDTO matchedFeaturegroup = findFeature(feature, featurestore, featuregroupDTOS);
    String sqlStr = "SELECT " + feature + " FROM " +
        getTableName(matchedFeaturegroup.getName(), matchedFeaturegroup.getVersion());
    return logAndRunSQL(sparkSession, sqlStr);
  }

  /**
   * Gets a feature from a featurestore and a specific featuregroup.
   *
   * @param sparkSession        the spark session
   * @param feature             the feature to get
   * @param featurestore        the featurestore to query
   * @param featuregroup        the featuregroup where the feature is located
   * @param featuregroupVersion the version of the featuregroup
   * @return the resulting spark dataframe with the feature
   */
  public static Dataset<Row> getFeature(SparkSession sparkSession, String feature, String featurestore,
                                        String featuregroup, int featuregroupVersion) {
    useFeaturestore(sparkSession, featurestore);
    String sqlStr = "SELECT " + feature + " FROM " + getTableName(featuregroup, featuregroupVersion);
    return logAndRunSQL(sparkSession, sqlStr);
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
   * Converts a map of (featuregroup --> version) to a list of featuregroupDTOs
   *
   * @param featuregroupsAndVersions the map of (featuregroup --> version)
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
   * @param featuregroupsAndVersions a map of (featuregroup --> version) where the featuregroups are located
   * @param joinKey                  the key to join on
   * @return the resulting spark dataframe with the features
   */
  public static Dataset<Row> getFeatures(SparkSession sparkSession, List<String> features, String featurestore,
                                         Map<String, Integer> featuregroupsAndVersions, String joinKey) {
    useFeaturestore(sparkSession, featurestore);
    List<FeaturegroupDTO> featuregroupDTOs = convertFeaturegroupAndVersionToDTOs(featuregroupsAndVersions);
    useFeaturestore(sparkSession, featurestore);
    String featuresStr = StringUtils.join(features, ", ");
    List<String> featuregroupStrings = new ArrayList<>();
    for (Map.Entry<String, Integer> entry : featuregroupsAndVersions.entrySet()) {
      featuregroupStrings.add(getTableName(entry.getKey(), entry.getValue()));
    }
    String featuregroupStr = StringUtils.join(featuregroupStrings, ", ");
    if (featuregroupsAndVersions.size() == 1) {
      String sqlStr = "SELECT " + featuresStr + " FROM " + featuregroupStr;
      return logAndRunSQL(sparkSession, sqlStr);
    } else {
      SQLJoinDTO sqlJoinDTO = getJoinStr(featuregroupDTOs, joinKey);
      String sqlStr = "SELECT " + featuresStr + " FROM " +
          getTableName(sqlJoinDTO.getFeaturegroupDTOS().get(0).getName(),
              sqlJoinDTO.getFeaturegroupDTOS().get(0).getVersion())
          + " " + sqlJoinDTO.getJoinStr();
      return logAndRunSQL(sparkSession, sqlStr);
    }
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
   * @return the resulting spark dataframe with the features
   */
  public static Dataset<Row> getFeatures(SparkSession sparkSession, List<String> features,
                                         String featurestore, List<FeaturegroupDTO> featuregroupsMetadata,
                                         String joinKey) {
    useFeaturestore(sparkSession, featurestore);
    String featuresStr = StringUtils.join(features, ", ");
    List<FeaturegroupDTO> featureFeatureGroups = new ArrayList<>();
    Map<String, FeaturegroupDTO> featuresToFeaturegroup = new HashMap<>();
    for (String feature : features) {
      FeaturegroupDTO featuregroupMatched = findFeature(feature, featurestore, featuregroupsMetadata);
      featuresToFeaturegroup.put(feature, featuregroupMatched);
      featureFeatureGroups.add(featuregroupMatched);
    }
    List<String> featuregroupStrings =
        featuregroupsMetadata.stream()
            .map(fg -> getTableName(fg.getName(), fg.getVersion())).collect(Collectors.toList());
    String featuregroupStr = StringUtils.join(featuregroupStrings, ", ");
    if (featuregroupsMetadata.size() == 1) {
      String sqlStr = "SELECT " + featuresStr + " FROM " + featuregroupStr;
      return logAndRunSQL(sparkSession, sqlStr);
    } else {
      SQLJoinDTO sqlJoinDTO = getJoinStr(featuregroupsMetadata, joinKey);
      String sqlStr = "SELECT " + featuresStr + " FROM " +
          getTableName(sqlJoinDTO.getFeaturegroupDTOS().get(0).getName(),
              sqlJoinDTO.getFeaturegroupDTOS().get(0).getVersion())
          + " " + sqlJoinDTO.getJoinStr();
      return logAndRunSQL(sparkSession, sqlStr);
    }
  }


  /**
   * Runs an SQL query on the project's featurestore
   *
   * @param sparkSession the spark session
   * @param query        the query to run
   * @param featurestore the featurestore to query
   * @return the resulting Spark dataframe
   */
  public static Dataset<Row> queryFeaturestore(SparkSession sparkSession, String query, String featurestore) {
    useFeaturestore(sparkSession, featurestore);
    return logAndRunSQL(sparkSession, query);
  }

  /**
   * Runs an SQL query with SparkSQL and logs it
   *
   * @param sparkSession the spark session
   * @param sqlStr       the query to run
   * @return the resulting spark dataframe
   */
  private static Dataset<Row> logAndRunSQL(SparkSession sparkSession, String sqlStr) {
    LOG.log(Level.INFO, "Running sql: " + sqlStr);
    return sparkSession.sql(sqlStr);
  }

  /**
   * Filters a list of featuregroups based on a user-provided map of featuregroup --> version
   *
   * @param featuregroupsAndVersions the map of featuregroup --> version
   * @param featuregroupsMetadata    the list of featuregroups to filter
   * @return filtered list of featuregroups
   */
  public static List<FeaturegroupDTO> filterFeaturegroupsBasedOnMap(
      Map<String, Integer> featuregroupsAndVersions, List<FeaturegroupDTO> featuregroupsMetadata) {
    return featuregroupsMetadata.stream().filter(
        fgm -> featuregroupsAndVersions.get(fgm.getName()) != null
            && fgm.getVersion().equals(featuregroupsAndVersions.get(fgm.getName()))).collect(Collectors.toList());
  }

  /**
   * Converts the name of a spark datatype to the corresponding hive datatype
   *
   * @param sparkDtype the spark datatype name to convert
   * @return the hive datatype name
   */
  private static String convertSparkDTypeToHiveDtype(String sparkDtype) {
    if (Constants.HIVE_DATA_TYPES.contains(sparkDtype.toUpperCase())) {
      return sparkDtype.toUpperCase();
    }
    if (sparkDtype.equalsIgnoreCase("long"))
      return "BIGINT";
    if (sparkDtype.equalsIgnoreCase("short"))
      return "INT";
    if (sparkDtype.equalsIgnoreCase("byte"))
      return "CHAR";
    if (sparkDtype.equalsIgnoreCase("integer"))
      return "INT";
    return null;
  }

  /**
   * Converts a spark schema field into a FeatureDTO
   *
   * @param field      the field to convert
   * @param primaryKey the name of the primary key for the featuregroup where the feature belongs
   * @return the converted featureDTO
   */
  private static FeatureDTO convertFieldToFeature(StructField field, String primaryKey) {
    String featureName = field.name();
    String featureType = convertSparkDTypeToHiveDtype(field.dataType().typeName());
    String featureDesc = "";
    Boolean primary = false;
    if (primaryKey != null && featureName.equalsIgnoreCase(primaryKey)) {
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
    return new FeatureDTO(featureName, featureType, featureDesc, primary);
  }

  /**
   * Parses a spark schema into a list of FeatureDTOs
   *
   * @param sparkSchema the spark schema to parse
   * @param primaryKey  the name of the primary key for the featuregroup where the feature belongs
   * @return a list of feature DTOs
   */
  public static List<FeatureDTO> parseSparkFeaturesSchema(StructType sparkSchema, String primaryKey) {
    StructField[] fieldsList = sparkSchema.fields();
    List<FeatureDTO> features = new ArrayList<>();
    for (int i = 0; i < fieldsList.length; i++) {
      features.add(convertFieldToFeature(fieldsList[i], primaryKey));
    }
    return features;
  }

  /**
   * Utility method for getting the default primary key of a featuregroup that is to be created if no other primary
   * key have been provided by the user
   *
   * @param featuregroupDf the spark dataframe to create the featuregroup from
   * @return the name of the column that is the primary key
   */
  public static String getDefaultPrimaryKey(Dataset<Row> featuregroupDf) {
    return featuregroupDf.dtypes()[0]._1;
  }

  /**
   * Utility method for validating a primary key provided by a user for a new featuregroup
   *
   * @param featuregroupDf the spark dataframe to create the featuregroup from
   * @param primaryKey     the provided primary key
   * @return true or false depending on if primary key is valid or not
   * @throws InvalidPrimaryKeyForFeaturegroup InvalidPrimaryKeyForFeaturegroup
   */
  public static Boolean validatePrimaryKey(Dataset<Row> featuregroupDf, String primaryKey)
      throws InvalidPrimaryKeyForFeaturegroup {
    List<String> columns = new ArrayList<>();
    for (int i = 0; i < featuregroupDf.dtypes().length; i++) {
      columns.add(featuregroupDf.dtypes()[i]._1.toLowerCase());
    }
    if (columns.contains(primaryKey.toLowerCase())) {
      return true;
    } else {
      throw new InvalidPrimaryKeyForFeaturegroup("Invalid primary Key: " + primaryKey
          + ", the specified primary key does not exist among the available columns: " +
          StringUtils.join(",", columns));
    }
  }

  /**
   * Validates metadata provided by the user when creating new feature groups and training datasets
   *
   * @param name the name of the feature group/training dataset
   * @param dtypes the schema of the provided spark dataframe
   * @param dependencies the list of data dependencies of the feature group / training dataset
   * @param description the description about the feature group/training dataset
   */
  public static void validateMetadata(String name, Tuple2<String, String>[] dtypes,
                                      List<String> dependencies, String description) {
    Pattern namePattern = Pattern.compile("^[a-zA-Z0-9-_]+$");
    if (name.length() > 256 || name.equals("") || !namePattern.matcher(name).matches())
      throw new IllegalArgumentException("Name of feature group/training dataset cannot be empty, " +
          "cannot exceed 256 characters, and must match the regular expression: ^[a-zA-Z0-9-_]+$" +
          " the provided name: " + name + " is not valid");

    if (dtypes.length == 0)
      throw new IllegalArgumentException("Cannot create a feature group from an empty spark dataframe");

    for (int i = 0; i < dtypes.length; i++) {
      if (dtypes[i]._1.length() > 767 || dtypes[i]._1.equals("") || !namePattern.matcher(dtypes[i]._1).matches())
        throw new IllegalArgumentException("Name of feature column cannot be empty, cannot exceed 767 characters," +
            " and must match the regular expression: ^[a-zA-Z0-9-_]+$, the provided feature name: " + dtypes[i]._1 +
            " is not valid");
    }
    if (!(new HashSet<>(dependencies).size() == dependencies.size())) {
      String dependenciesStr = StringUtils.join(dependencies, ",");
      throw new IllegalArgumentException("THe list of data dependencies contains duplicates: " + dependenciesStr);
    }

    if(description.length() > 2000)
      throw new IllegalArgumentException("Feature group/Training dataset description should not exceed " +
          "the maximum length of 2000 characters, " +
          "the provided description has length:" + dependencies.size());
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
   * Parses FeaturestoreMetadata JSON into a DTO
   *
   * @param featurestoreMetadata the JSON to parse
   * @return the DTO
   * @throws JAXBException JAXBException
   */
  public static FeaturegroupsAndTrainingDatasetsDTO parseFeaturestoreMetadataJson(JSONObject featurestoreMetadata)
      throws JAXBException {
    Unmarshaller unmarshaller = getUnmarshaller(featuregroupsAndTrainingDatasetsJAXBContext);
    StreamSource json = new StreamSource(new StringReader(featurestoreMetadata.toString()));
    return unmarshaller.unmarshal(json, FeaturegroupsAndTrainingDatasetsDTO.class).getValue();
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
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   */
  public static StatisticsDTO computeDataFrameStats(
      String name, SparkSession sparkSession, Dataset<Row> sparkDf, String featurestore,
      int version, Boolean descriptiveStatistics, Boolean featureCorrelation,
      Boolean featureHistograms, Boolean clusterAnalysis,
      List<String> statColumns, int numBins, int numClusters,
      String correlationMethod) throws DataframeIsEmpty, SparkDataTypeNotRecognizedError {
    if (sparkDf == null) {
      sparkDf = getFeaturegroup(sparkSession, name, featurestore, version);
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
            "set the optional argument descriptive_statistics=False to skip this step");
        throw e;
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
            "set the optional argument feature_correlation=False to skip this step");
        throw e;
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
            "set the optional argument feature_histograms=False to skip this step");
        throw e;
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
            "set the optional argument cluster_analysis=False to skip this step");
        throw e;
      }
    }
    return new StatisticsDTO(
        descriptiveStatsDTO, clusterAnalysisDTO, featureCorrelationMatrixDTO, featureDistributionsDTO);

  }

  /**
   * Materializes a training dataset dataframe to HDFS
   *
   * @param sparkSession the spark session
   * @param sparkDf      the spark dataframe to materialize
   * @param hdfsPath     the HDFS path
   * @param dataFormat   the format to serialize to
   * @param writeMode    the spark write mode (append/overwrite)
   * @throws TrainingDatasetFormatNotSupportedError if the provided dataframe format is not supported, supported formats
   * are tfrecords, csv, tsv, and parquet
   */
  public static void writeTrainingDatasetHdfs(SparkSession sparkSession, Dataset<Row> sparkDf,
                                              String hdfsPath, String dataFormat, String writeMode) throws
      TrainingDatasetFormatNotSupportedError {
    sparkSession.sparkContext().setJobGroup("Materializing dataframe as training dataset",
        "Saving training dataset in path: " + hdfsPath + ", in format: " + dataFormat, true);
    switch (dataFormat) {
      case Constants.TRAINING_DATASET_CSV_FORMAT:
        sparkDf.write().option(Constants.SPARK_WRITE_DELIMITER, Constants.COMMA_DELIMITER).mode(
            writeMode).option(Constants.SPARK_WRITE_HEADER, "true").csv(hdfsPath);
        sparkSession.sparkContext().setJobGroup("", "", true);
        break;
      case Constants.TRAINING_DATASET_TSV_FORMAT:
        sparkDf.write().option(Constants.SPARK_WRITE_DELIMITER, Constants.TAB_DELIMITER)
            .mode(writeMode).option(
            Constants.SPARK_WRITE_HEADER, "true").csv(hdfsPath);
        sparkSession.sparkContext().setJobGroup("", "", true);
        break;
      case Constants.TRAINING_DATASET_PARQUET_FORMAT:
        sparkDf.write().format(dataFormat).mode(writeMode).parquet(hdfsPath);
        sparkSession.sparkContext().setJobGroup("", "", true);
        break;
      case Constants.TRAINING_DATASET_TFRECORDS_FORMAT:
        sparkDf.write().format(dataFormat).option(Constants.SPARK_TF_CONNECTOR_RECORD_TYPE,
            Constants.SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE).mode(writeMode).save(hdfsPath);
        sparkSession.sparkContext().setJobGroup("", "", true);
        break;
      default:
        sparkSession.sparkContext().setJobGroup("", "", true);
        throw new TrainingDatasetFormatNotSupportedError("The provided dataformat: " + dataFormat +
            " is not supported, the supported modes are: " +
            Constants.TRAINING_DATASET_CSV_FORMAT + "," +
            Constants.TRAINING_DATASET_TSV_FORMAT + "," +
            Constants.TRAINING_DATASET_TFRECORDS_FORMAT + "," +
            Constants.TRAINING_DATASET_PARQUET_FORMAT + ",");
    }
  }

  /**
   * Finds a given training dataset from the featurestore metadata by looking for name and version
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
      throw new FeaturegroupDoesNotExistError("Could not find the requested feature grup with name: " +
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
    FSDataOutputStream outputStream = null;
    try {
      FileSystem hdfs = filePath.getFileSystem(hdfsConf);
      outputStream = hdfs.create(filePath, true);
      outputStream.writeBytes(tfRecordSchemaJson);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Could not save tf record schema json to HDFS", e);
    } finally {
      if (outputStream != null) {
        outputStream.flush();
        outputStream.close();
      }
    }
  }

  /**
   * Gets the TFRecords schema in JSON format for a spark dataframe
   *
   * @param sparkDf
   * @return the TFRecords schema as a JSONObject
   */
  public static JSONObject getDataframeTfRecordSchemaJson(Dataset<Row> sparkDf) {
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

}
