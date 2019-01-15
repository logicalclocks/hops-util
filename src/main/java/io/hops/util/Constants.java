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

package io.hops.util;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 * Constants used by Hops.
 */
public class Constants {

  public static final String HOPSWORKS_REST_APPSERVICE = "appservice";
  public static final String HOPSWORKS_REST_APPSERVICE_FEATURESTORE_RESOURCE = "featurestore";
  public static final String HOPSWORKS_REST_APPSERVICE_FEATURESTORES_RESOURCE = "featurestores";
  public static final String HOPSWORKS_REST_APPSERVICE_CLEAR_FEATUREGROUP_RESOURCE = "featurestore/featuregroup/clear";
  public static final String HOPSWORKS_REST_APPSERVICE_CREATE_FEATUREGROUP_RESOURCE = "featurestore/featuregroups";
  public static final String HOPSWORKS_REST_APPSERVICE_UPDATE_FEATUREGROUP_RESOURCE = "featurestore/featuregroup";
  public static final String HOPSWORKS_REST_APPSERVICE_CREATE_TRAINING_DATASET_RESOURCE =
      "featurestore/trainingdatasets";
  public static final String HOPSWORKS_REST_APPSERVICE_UPDATE_TRAINING_DATASET_RESOURCE =
      "featurestore/trainingdataset";

  public static final String PROJECTID_ENV_VAR = "hopsworks.projectid";
  public static final String CRYPTO_MATERIAL_PASSWORD = "material_passwd";
  public static final String K_CERTIFICATE_ENV_VAR = "k_certificate";
  public static final String DOMAIN_CA_TRUSTSTORE = "domain_ca_truststore";
  //System properties set by Hopsworks
  public static final String KAFKA_FLINK_PARAMS = "kafka_params";//used by hops-examples-flink
  public static final String HOPSWORKS_REST_RESOURCE = "hopsworks-api/api";
  public static final long WAIT_JOBS_INTERVAL = 5;
  public static final long WAIT_JOBS_TIMEOUT = 7;
  public static final TimeUnit WAIT_JOBS_TIMEOUT_TIMEUNIT = TimeUnit.DAYS;

  public static final String JOBTYPE_ENV_VAR = "hopsworks.job.type";
  public static final String T_CERTIFICATE_ENV_VAR = "t_certificate";
  public static final String HOPSWORKS_RESTENDPOINT = "hopsworks.restendpoint";
  public static final String JOBNAME_ENV_VAR = "hopsworks.job.name";
  public static final String APPID_ENV_VAR = "hopsworks.job.appid";
  public static final String PROJECTNAME_ENV_VAR = "hopsworks.projectname";
  public static final boolean WAIT_JOBS_RUNNING_STATE = true;
  public static final String KAFKA_TOPICS_ENV_VAR = "hopsworks.kafka.job.topics";
  public static final String ELASTIC_ENDPOINT_ENV_VAR = "hopsworks.elastic.endpoint";
  public static final String KAFKA_CONSUMER_GROUPS = "hopsworks.kafka.consumergroups";
  public static final String KAFKA_BROKERADDR_ENV_VAR = "hopsworks.kafka.brokeraddress";
  public static final String SERVER_TRUSTSTORE_PROPERTY = "server.truststore";

  //JSON properties sent to Hopsworks REST API
  public static final String JSON_JOBSTATE = "running";
  public static final String JSON_JOBIDS = "jobIds";
  public static final String JSON_KEYSTOREPWD = "keyStorePwd";
  public static final String JSON_SCHEMA_CONTENTS = "contents";
  public static final String JSON_SCHEMA_TOPICNAME = "topicName";
  public static final String JSON_SCHEMA_VERSION = "version";
  public static final String JSON_KEYSTORE = "keyStore";

  public static final String JSON_FEATURESTORE_NAME = "featurestoreName";

  public static final String JSON_FEATUREGROUP_NAME = "name";
  public static final String JSON_FEATUREGROUP_VERSION = "version";
  public static final String JSON_FEATUREGROUP_JOBNAME = "jobName";
  public static final String JSON_FEATUREGROUP_FEATURES = "features";
  public static final String JSON_FEATUREGROUP_DEPENDENCIES = "dependencies";
  public static final String JSON_FEATUREGROUP_DESCRIPTION = "description";
  public static final String JSON_FEATUREGROUP_FEATURE_CORRELATION = "featureCorrelationMatrix";
  public static final String JSON_FEATUREGROUP_DESC_STATS = "descriptiveStatistics";
  public static final String JSON_FEATUREGROUP_UPDATE_METADATA = "updateMetadata";
  public static final String JSON_FEATUREGROUP_UPDATE_STATS = "updateStats";
  public static final String JSON_FEATUREGROUP_FEATURES_HISTOGRAM = "featuresHistogram";
  public static final String JSON_FEATUREGROUP_CLUSTER_ANALYSIS = "clusterAnalysis";
  public static final String JSON_FEATUREGROUPS = "featuregroups";

  public static final String JSON_FEATURE_NAME = "name";
  public static final String JSON_FEATURE_TYPE = "type";
  public static final String JSON_FEATURE_DESCRIPTION = "description";
  public static final String JSON_FEATURE_PRIMARY = "primary";

  public static final String JSON_TRAINING_DATASET_NAME = "name";
  public static final String JSON_TRAINING_DATASETS = "trainingDatasets";
  public static final String JSON_TRAINING_DATASET_HDFS_STORE_PATH = "hdfsStorePath";
  public static final String JSON_TRAINING_DATASET_FORMAT = "dataFormat";
  public static final String JSON_TRAINING_DATASET_SCHEMA = "features";
  public static final String JSON_TRAINING_DATASET_VERSION = "version";
  public static final String JSON_TRAINING_DATASET_DEPENDENCIES = "dependencies";
  public static final String JSON_TRAINING_DATASET_DESCRIPTION = "description";
  public static final String JSON_TRAINING_DATASET_FEATURE_CORRELATION = "featureCorrelationMatrix";
  public static final String JSON_TRAINING_DATASET_FEATURES_HISTOGRAM = "featuresHistogram";
  public static final String JSON_TRAINING_DATASET_CLUSTER_ANALYSIS = "clusterAnalysis";
  public static final String JSON_TRAINING_DATASET_DESC_STATS = "descriptiveStatistics";
  public static final String JSON_TRAINING_DATASET_JOBNAME = "jobName";
  public static final String JSON_TRAINING_DATASET_UPDATE_METADATA = "updateMetadata";
  public static final String JSON_TRAINING_DATASET_UPDATE_STATS = "updateStats";

  public static final String JSON_ERROR_CODE = "errorCode";
  public static final String JSON_ERROR_MSG = "errorMsg";
  public static final String JSON_USR_MSG = "usrMsg";

  public static final String JSON_DESCRIPTIVE_STATS_FEATURE_NAME= "featureName";
  public static final String JSON_DESCRIPTIVE_STATS_METRIC_VALUES= "metricValues";
  public static final String JSON_DESCRIPTIVE_STATS= "descriptiveStats";

  public static final String JSON_CLUSTERING_ANALYSIS_DATA_POINT_NAME = "datapointName";
  public static final String JSON_CLUSTERING_ANALYSIS_FIRST_DIMENSION = "firstDimension";
  public static final String JSON_CLUSTERING_ANALYSIS_SECOND_DIMENSION = "secondDimension";
  public static final String JSON_CLUSTERING_ANALYSIS_CLUSTER = "cluster";
  public static final String JSON_CLUSTERING_ANALYSIS_CLUSTERS = "clusters";
  public static final String JSON_CLUSTERING_ANALYSIS_DATA_POINTS = "dataPoints";

  public static final String JSON_HISTOGRAM_FREQUENCY = "frequency";
  public static final String JSON_HISTOGRAM_BIN = "bin";
  public static final String JSON_HISTOGRAM_FEATURE_NAME = "featureName";
  public static final String JSON_HISTOGRAM_FREQUENCY_DISTRIBUTION = "frequencyDistribution";
  public static final String JSON_HISTOGRAM_FEATURE_DISTRIBUTIONS = "featureDistributions";

  public static final String JSON_CORRELATION_FEATURE_NAME = "featureName";
  public static final String JSON_CORRELATION = "correlation";
  public static final String JSON_CORRELATION_VALUES = "correlationValues";

  public static final String JSON_FEATURE_CORRELATIONS = "featureCorrelations";

  //SPARK Config properties
  public static final String SPARK_SCHEMA_FIELD_METADATA = "metadata";
  public static final String SPARK_SCHEMA_FIELDS = "fields";
  public static final String SPARK_SCHEMA_FIELD_NAME = "name";
  public static final String SPARK_SCHEMA_FIELD_TYPE = "type";
  public static final List<String> NUMERIC_SPARK_TYPES = Arrays.asList("bigint", "decimal", "integer", "int",
      "double", "long", "float", "short");
  public static final String SPARK_OVERWRITE_MODE = "overwrite";
  public static final String SPARK_WRITE_DELIMITER = "delimiter";
  public static final String SPARK_WRITE_HEADER = "header";
  public static final String SPARK_TF_CONNECTOR_RECORD_TYPE = "recordType";
  public static final String SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE = "Example";
  public static final String SPARK_LONG_TYPE = "long";
  public static final String SPARK_SHORT_TYPE = "short";
  public static final String SPARK_BYTE_TYPE = "byte";
  public static final String SPARK_INTEGER_TYPE = "integer";

  //Hive Config
  public static final List<String> HIVE_DATA_TYPES = Arrays.asList("TINYINT", "SMALLINT", "INT", "BIGINT",
      "FLOAT", "DOUBLE", "DECIMAL", "TIMESTAMP", "DATE", "INTERVAL", "STRING", "VARCHAR", "CHAR",
      "BOOLEAN", "BINARY", "ARRAY", "MAP", "STRUCT", "UNIONTYPE");
  public static final String HIVE_BIGINT_TYPE = "BIGINT";
  public static final String HIVE_INT_TYPE = "INT";
  public static final String HIVE_CHAR_TYPE = "CHAR";

  public static final String PROJECT_STAGING_DIR = "Resources";
  public static final String PROJECT_ROOT_DIR = "Projects";

  //Featurestore properties

  public static final String TRAINING_DATASET_CSV_FORMAT = "csv";
  public static final String TRAINING_DATASET_TSV_FORMAT = "tsv";
  public static final String TRAINING_DATASET_PARQUET_FORMAT = "parquet";
  public static final String TRAINING_DATASET_TFRECORDS_FORMAT = "tfrecords";
  public static final String TRAINING_DATASET_CSV_SUFFIX = ".csv";
  public static final String TRAINING_DATASET_TSV_SUFFIX = ".tsv";
  public static final String TRAINING_DATASET_PARQUET_SUFFIX = ".parquet";
  public static final String TRAINING_DATASET_TFRECORDS_SUFFIX = ".tfrecords";
  public static final String CLUSTERING_ANALYSIS_INPUT_COLUMN = "featurestore_feature_clustering_analysis_input_col";
  public static final String CLUSTERING_ANALYSIS_OUTPUT_COLUMN = "featurestore_feature_clustering_analysis_output_col";
  public static final String CLUSTERING_ANALYSIS_PCA_COLUMN = "featurestore_feature_clustering_analysis_pca_col";
  public static final String CLUSTERING_ANALYSIS_FEATURES_COLUMN = "features";
  public static final String CLUSTERING_ANALYSIS_CLUSTERS_OUTPUT_COLUMN = "clusters";
  public static final String CLUSTERING_ANALYSIS_CLUSTERS_COLUMN = "featurestore_feature_clustering_analysis_pca_col";
  public static final String CLUSTERING_ANALYSIS_VALUES_COLUMN = "values";
  public static final int CLUSTERING_ANALYSIS_SAMPLE_SIZE = 50;
  public static final String FEATURE_GROUP_INSERT_APPEND_MODE = "append";
  public static final String FEATURE_GROUP_INSERT_OVERWRITE_MODE = "overwrite";
  public static final String DESCRIPTIVE_STATS_SUMMARY_COL= "summary";
  public static final String DESCRIPTIVE_STATS_METRIC_NAME_COL= "metricName";
  public static final String DESCRIPTIVE_STATS_VALUE_COL= "value";
  public static final String HISTOGRAM_FREQUENCY = "frequency";
  public static final String HISTOGRAM_FEATURE = "feature";
  public static final String FEATURESTORE_SUFFIX =  "_featurestore";
  public static final String CORRELATION_ANALYSIS_INPUT_COLUMN = "featurestore_feature_correlation_analysis_input_col";
  public static final String CORRELATION_ANALYSIS_DEFAULT_METHOD = "pearson";
  public static final String TF_RECORD_SCHEMA_FEATURE = "feature";
  public static final String TF_RECORD_SCHEMA_FEATURE_FIXED = "fixed_len";
  public static final String TF_RECORD_SCHEMA_FEATURE_VAR = "var_len";
  public static final String TF_RECORD_SCHEMA_TYPE = "type";
  public static final String TF_RECORD_INT_TYPE = "int";
  public static final String TF_RECORD_FLOAT_TYPE = "float";
  public static final String TF_RECORD_STRING_TYPE = "string";
  public static final String TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME = "tf_record_schema.txt";

  public static final String SLASH_DELIMITER = "/";
  public static final String COMMA_DELIMITER = ",";
  public static final String TAB_DELIMITER = "\t";
  public static final String HDFS_DEFAULT = "hdfs://default";
  public static final int MAX_CORRELATION_MATRIX_COLUMNS = 50;

}
