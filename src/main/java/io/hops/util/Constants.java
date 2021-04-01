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
  
  public static final String HOPSWORKS_REST_PROJECT_RESOURCE = "project";
  public static final String HOPSWORKS_CLOUD_RESOURCE = "cloud";
  public static final String HOPSWORKS_AWS_CLOUD_SESSION_TOKEN_RESOURCE = "aws/session-token";
  public static final String HOPSWORKS_CLOUD_ROLE_MAPPINGS_RESOURCE = "role-mappings";
  public static final String HOPSWORKS_CLOUD_SESSION_TOKEN_RESOURCE_QUERY_ROLE = "roleARN";
  public static final String HOPSWORKS_CLOUD_SESSION_TOKEN_RESOURCE_QUERY_SESSION = "roleSessionName";
  public static final String HOPSWORKS_CLOUD_SESSION_TOKEN_RESOURCE_QUERY_SESSION_DURATION = "durationSeconds";
  
  public static final String JSON_ACCESS_KEY_ID = "accessKeyId";
  public static final String JSON_SECRET_ACCESS_KEY_ID = "secretAccessKey";
  public static final String JSON_SESSION_TOKEN_ID = "sessionToken";
  public static final String JSON_ARRAY_ITEMS = "items";
  public static final String JSON_CLOUD_ROLE = "cloudRole";

  public static final String PROJECTID_ENV_VAR = "hopsworks.projectid";
  public static final String CRYPTO_MATERIAL_PASSWORD = "material_passwd";
  public static final String K_CERTIFICATE_ENV_VAR = "k_certificate";
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
  public static final String DOMAIN_CA_TRUSTSTORE = "hopsworks.domain.truststore";
  
  //JSON properties sent to Hopsworks REST API
  public static final String JSON_JOBSTATE = "running";
  public static final String JSON_KEYSTOREPWD = "keyStorePwd";
  public static final String JSON_SCHEMA_CONTENTS = "contents";
  public static final String JSON_SCHEMA_TOPICNAME = "topicName";
  public static final String JSON_SCHEMA_VERSION = "version";
  public static final String JSON_KEYSTORE = "keyStore";

  public static final String JSON_ERROR_CODE = "errorCode";
  public static final String JSON_ERROR_MSG = "errorMsg";
  public static final String JSON_USR_MSG = "usrMsg";

  //SPARK Config properties
  public static final List<String> NUMERIC_SPARK_TYPES = Arrays.asList("bigint", "decimal", "integer", "int",
      "double", "long", "float", "short");
  public static final String SPARK_OVERWRITE_MODE = "overwrite";
  public static final String SPARK_APPEND_MODE = "append";
  public static final String SPARK_WRITE_DELIMITER = "delimiter";
  public static final String SPARK_WRITE_HEADER = "header";
  public static final String SPARK_INFER_SCHEMA = "inferSchema";
  public static final String SPARK_TF_CONNECTOR_RECORD_TYPE = "recordType";
  public static final String SPARK_TF_CONNECTOR_RECORD_TYPE_EXAMPLE = "Example";
  public static final String SPARK_SQL_CATALOG_IMPLEMENTATION = "spark.sql.catalogImplementation";
  public static final String SPARK_SQL_CATALOG_HIVE = "hive";
  
  //Hive Config
  public static final String PROJECT_STAGING_DIR = "Resources";
  public static final String PROJECT_ROOT_DIR = "Projects";

  //Featurestore properties
  public static final String SLASH_DELIMITER = "/";
  public static final String S3_ACCESS_KEY_ENV = "fs.s3a.access.key";
  public static final String S3_SECRET_KEY_ENV = "fs.s3a.secret.key";
  public static final String S3_SESSION_KEY_ENV = "fs.s3a.session.token";
  public static final String S3_CREDENTIAL_PROVIDER_ENV = "fs.s3a.aws.credentials.provider";
  public static final String S3_TEMPORARY_CREDENTIAL_PROVIDER =
    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";
  public static final String SPARK_IS_DRIVER_ENV = "IS_HOPS_DRIVER";
  public static final String AWS_ACCESS_KEY_ID_ENV = "AWS_ACCESS_KEY_ID";
  public static final String AWS_SECRET_ACCESS_KEY_ENV = "AWS_SECRET_ACCESS_KEY";
  public static final String AWS_SESSION_TOKEN_ENV = "AWS_SESSION_TOKEN";

  public static final String HOPSWORKS_REST_ELASTIC_RESOURCE = "elastic";
  public static final String HOPSWORKS_REST_JWT_RESOURCE = "jwt";
  
}
