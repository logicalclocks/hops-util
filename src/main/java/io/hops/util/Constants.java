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

import java.util.concurrent.TimeUnit;

/**
 *
 * Constants used by Hops.
 */
public class Constants {

  public static final String HOPSWORKS_REST_APPSERVICE = "appservice";
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

  public static final String PROJECT_STAGING_DIR = "Resources";
  public static final String PROJECT_ROOT_DIR = "Projects";
}
