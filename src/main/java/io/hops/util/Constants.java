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
  
  public static final String HOPSWORKS_REST_FEATURESTORES_RESOURCE = "featurestores";
  public static final String HOPSWORKS_REST_FEATURESTORE_METADATA_RESOURCE = "metadata";
  public static final String HOPSWORKS_REST_FEATUREGROUPS_RESOURCE = "featuregroups";
  public static final String HOPSWORKS_REST_TRAININGDATASETS_RESOURCE = "trainingdatasets";
  public static final String HOPSWORKS_REST_FEATUREGROUP_CLEAR_RESOURCE = "clear";
  
  public static final String PROJECTID_ENV_VAR = "hopsworks.projectid";
  public static final String CRYPTO_MATERIAL_PASSWORD = "material_passwd";
  public static final String K_CERTIFICATE_ENV_VAR = "k_certificate";
  public static final String DOMAIN_CA_TRUSTSTORE = "domain_ca_truststore";
  public static final String JWT_FILENAME = "token.jwt";
  //System properties set by Hopsworks
  public static final String KAFKA_FLINK_PARAMS = "kafka_params";//used by hops-examples-flink
  public static final String HOPSWORKS_REST_RESOURCE = "hopsworks-api/api";
  public static final long WAIT_JOBS_INTERVAL = 5;
  public static final TimeUnit WAIT_JOBS_INTERVAL_TIMEUNIT = TimeUnit.SECONDS;
  public static final long WAIT_JOBS_TIMEOUT = 7;
  public static final TimeUnit WAIT_JOBS_TIMEOUT_TIMEUNIT = TimeUnit.DAYS;

  public static final String JOBTYPE_ENV_VAR = "hopsworks.job.type";
  public static final String T_CERTIFICATE_ENV_VAR = "t_certificate";
  public static final String HOPSWORKS_RESTENDPOINT = "hopsworks.restendpoint";
  public static final String HOPSUTIL_INSECURE = "hopsutil.insecure";
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
  public static final String JSON_KEYSTOREPWD = "keyStorePwd";
  public static final String JSON_SCHEMA_CONTENTS = "contents";
  public static final String JSON_SCHEMA_TOPICNAME = "topicName";
  public static final String JSON_SCHEMA_VERSION = "version";
  public static final String JSON_KEYSTORE = "keyStore";

  public static final String JSON_FEATURESTORE_NAME = "featurestoreName";
  public static final String JSON_FEATURESTORE_ENTITY_TYPE = "type";

  public static final String JSON_FEATURESTORE_UPDATE_STATS_QUERY_PARAM = "updateStats";
  public static final String JSON_FEATURESTORE_UPDATE_METADATA_QUERY_PARAM = "updateMetadata";
  public static final String JSON_FEATUREGROUP_NAME = "name";
  public static final String JSON_FEATUREGROUP_VERSION = "version";
  public static final String JSON_FEATUREGROUP_JOBNAME = "jobName";
  public static final String JSON_FEATUREGROUP_FEATURES = "features";
  public static final String JSON_FEATUREGROUP_DESCRIPTION = "description";
  public static final String JSON_FEATUREGROUP_FEATURE_CORRELATION = "featureCorrelationMatrix";
  public static final String JSON_FEATUREGROUP_DESC_STATS = "descriptiveStatistics";
  public static final String JSON_FEATUREGROUP_UPDATE_METADATA = "updateMetadata";
  public static final String JSON_FEATUREGROUP_UPDATE_STATS = "updateStats";
  public static final String JSON_FEATUREGROUP_FEATURES_HISTOGRAM = "featuresHistogram";
  public static final String JSON_FEATUREGROUP_CLUSTER_ANALYSIS = "clusterAnalysis";
  public static final String JSON_FEATUREGROUP_TYPE =  "featuregroupType";
  
  public static final String JSON_FEATURE_DESCRIPTION = "description";

  public static final String JSON_TRAINING_DATASET_NAME = "name";
  public static final String JSON_TRAINING_DATASET_FORMAT = "dataFormat";
  public static final String JSON_TRAINING_DATASET_SCHEMA = "features";
  public static final String JSON_TRAINING_DATASET_VERSION = "version";

  public static final String JSON_TRAINING_DATASET_DESCRIPTION = "description";
  public static final String JSON_TRAINING_DATASET_FEATURE_CORRELATION = "featureCorrelationMatrix";
  public static final String JSON_TRAINING_DATASET_FEATURES_HISTOGRAM = "featuresHistogram";
  public static final String JSON_TRAINING_DATASET_CLUSTER_ANALYSIS = "clusterAnalysis";
  public static final String JSON_TRAINING_DATASET_DESC_STATS = "descriptiveStatistics";
  public static final String JSON_TRAINING_DATASET_JOBNAME = "jobName";
  public static final String JSON_TRAINING_DATASET_UPDATE_METADATA = "updateMetadata";
  public static final String JSON_TRAINING_DATASET_S3_CONNECTOR_ID = "s3ConnectorId";
  public static final String JSON_TRAINING_DATASET_HOPSFS_CONNECTOR_ID = "hopsfsConnectorId";
  public static final String JSON_TRAINING_DATASET_TYPE =  "trainingDatasetType";

  public static final String JSON_ERROR_CODE = "errorCode";
  public static final String JSON_ERROR_MSG = "errorMsg";
  public static final String JSON_USR_MSG = "usrMsg";
  

  //SPARK Config properties
  public static final List<String> NUMERIC_SPARK_TYPES = Arrays.asList("bigint", "decimal", "integer", "int",
      "double", "long", "float", "short");
  public static final String SPARK_OVERWRITE_MODE = "overwrite";
  public static final String SPARK_WRITE_DELIMITER = "delimiter";
  public static final String SPARK_WRITE_HEADER = "header";
  public static final String SPARK_TF_CONNECTOR_RECORD_TYPE = "recordType";
  public static final String SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE = "Example";
  public static final String SPARK_SQL_CATALOG_IMPLEMENTATION = "spark.sql.catalogImplementation";
  public static final String SPARK_SQL_CATALOG_HIVE = "hive";
  

  //Hive Config
  public static final String PROJECT_STAGING_DIR = "Resources";
  public static final String PROJECT_ROOT_DIR = "Projects";

  //Featurestore properties
  public static final String FEATURESTORE_SUFFIX =  "_featurestore";
  public static final String TRAINING_DATASETS_SUFFIX =  "_Training_Datasets";
  public static final String TRAINING_DATASET_CSV_FORMAT = "csv";
  public static final String TRAINING_DATASET_TSV_FORMAT = "tsv";
  public static final String TRAINING_DATASET_PARQUET_FORMAT = "parquet";
  public static final String TRAINING_DATASET_AVRO_FORMAT = "avro";
  public static final String TRAINING_DATASET_IMAGE_FORMAT = "image";
  public static final String TRAINING_DATASET_ORC_FORMAT = "orc";
  public static final String TRAINING_DATASET_TFRECORDS_FORMAT = "tfrecords";
  public static final String TRAINING_DATASET_CSV_SUFFIX = ".csv";
  public static final String TRAINING_DATASET_TSV_SUFFIX = ".tsv";
  public static final String TRAINING_DATASET_PARQUET_SUFFIX = ".parquet";
  public static final String TRAINING_DATASET_AVRO_SUFFIX = ".avro";
  public static final String TRAINING_DATASET_ORC_SUFFIX = ".orc";
  public static final String TRAINING_DATASET_IMAGE_SUFFIX = ".image";
  public static final String TRAINING_DATASET_TFRECORDS_SUFFIX = ".tfrecords";
  public static final String CLUSTERING_ANALYSIS_INPUT_COLUMN = "featurestore_feature_clustering_analysis_input_col";
  public static final String CLUSTERING_ANALYSIS_OUTPUT_COLUMN = "featurestore_feature_clustering_analysis_output_col";
  public static final String CLUSTERING_ANALYSIS_PCA_COLUMN = "featurestore_feature_clustering_analysis_pca_col";
  public static final String CLUSTERING_ANALYSIS_FEATURES_COLUMN = "features";
  public static final String CLUSTERING_ANALYSIS_CLUSTERS_OUTPUT_COLUMN = "clusters";
  public static final String CLUSTERING_ANALYSIS_VALUES_COLUMN = "values";
  public static final int CLUSTERING_ANALYSIS_SAMPLE_SIZE = 50;
  public static final String DESCRIPTIVE_STATS_SUMMARY_COL= "summary";
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
  public static final String JDBC_TRUSTSTORE_ARG = "sslTrustStore";
  public static final String JDBC_TRUSTSTORE_PW_ARG = "trustStorePassword";
  public static final String JDBC_KEYSTORE_ARG = "sslKeyStore";
  public static final String JDBC_KEYSTORE_PW_ARG = "keyStorePassword";
  public static final String SPARK_JDBC_FORMAT = "jdbc";
  public static final String SPARK_JDBC_URL = "url";
  public static final String SPARK_JDBC_DBTABLE = "dbtable";

  public static final String JDBC_CONNECTION_STRING_DELIMITER = ";";
  public static final String JDBC_CONNECTION_STRING_VALUE_DELIMITER = "=";
  public static final String SLASH_DELIMITER = "/";
  public static final String COMMA_DELIMITER = ",";
  public static final String TAB_DELIMITER = "\t";
  public static final String AMPERSAND_DELIMITER = "&";
  public static final String HDFS_DEFAULT = "hdfs://default";
  public static final int MAX_CORRELATION_MATRIX_COLUMNS = 50;


}
