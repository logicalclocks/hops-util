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

import com.google.common.io.ByteStreams;
import io.hops.util.exceptions.CannotWriteImageDataFrameException;
import io.hops.util.exceptions.CredentialsNotFoundException;
import io.hops.util.exceptions.DataframeIsEmpty;
import io.hops.util.exceptions.FeaturegroupCreationError;
import io.hops.util.exceptions.FeaturegroupDeletionError;
import io.hops.util.exceptions.FeaturegroupUpdateStatsError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.FeaturestoresNotFound;
import io.hops.util.exceptions.HTTPSClientInitializationException;
import io.hops.util.exceptions.InvalidPrimaryKeyForFeaturegroup;
import io.hops.util.exceptions.SchemaNotFoundException;
import io.hops.util.exceptions.SparkDataTypeNotRecognizedError;
import io.hops.util.exceptions.TrainingDatasetCreationError;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.exceptions.TrainingDatasetFormatNotSupportedError;
import io.hops.util.featurestore.FeaturegroupsAndTrainingDatasetsDTO;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.feature.FeatureDTO;
import io.hops.util.featurestore.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.stats.StatisticsDTO;
import io.hops.util.featurestore.trainingdataset.TrainingDatasetDTO;
import org.apache.avro.Schema;
import org.apache.commons.net.util.Base64;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Utility class to be used by applications that want to communicate with Hopsworks.
 * Users can call the getters within their Hopsworks jobs to get the provided properties.
 */
public class Hops {

  private static final Logger LOG = Logger.getLogger(Hops.class.getName());

  private static Integer projectId;
  private static String projectName;
  private static String jobName;
  private static String appId;
  private static String jobType;
  private static List<String> brokerEndpointsList;
  private static String brokerEndpoints;
  private static String restEndpoint;
  private static String keyStore;
  private static String trustStore;
  private static String keystorePwd;
  private static String truststorePwd;
  private static String elasticEndPoint;

  private static WorkflowManager workflowManager;

  static {
    setup();
  }

  private Hops() {

  }

  /**
   * Setup the static Hops instance upon instantiation.
   * <p>
   */
  private static synchronized void setup() {
    Properties sysProps = System.getProperties();
    //If the sysProps are properly set, it is a Spark job. Flink jobs must call the setup method.
    if (sysProps.containsKey(Constants.JOBTYPE_ENV_VAR) &&
      !sysProps.getProperty(Constants.JOBTYPE_ENV_VAR).equalsIgnoreCase("flink")) {
      restEndpoint = sysProps.getProperty(Constants.HOPSWORKS_RESTENDPOINT);
      projectName = sysProps.getProperty(Constants.PROJECTNAME_ENV_VAR);
      keyStore = Constants.K_CERTIFICATE_ENV_VAR;
      trustStore = Constants.T_CERTIFICATE_ENV_VAR;
    
      //Get keystore and truststore passwords from Hopsworks
      projectId = Integer.parseInt(sysProps.getProperty(Constants.PROJECTID_ENV_VAR));
      String pwd = getCertPw();
      keystorePwd = pwd;
      truststorePwd = pwd;
      jobName = sysProps.getProperty(Constants.JOBNAME_ENV_VAR);
      appId = sysProps.getProperty(Constants.APPID_ENV_VAR);
      jobType = sysProps.getProperty(Constants.JOBTYPE_ENV_VAR);
    
      elasticEndPoint = sysProps.getProperty(Constants.ELASTIC_ENDPOINT_ENV_VAR);
      //
      if (sysProps.containsKey(Constants.KAFKA_BROKERADDR_ENV_VAR)) {
        parseBrokerEndpoints(sysProps.getProperty(Constants.KAFKA_BROKERADDR_ENV_VAR));
      }
    }
    try {
      updateFeaturestoreMetadataCache(FeaturestoreHelper.getProjectFeaturestore());
    } catch (CredentialsNotFoundException | FeaturestoreNotFound | JAXBException e) {
      LOG.log(Level.SEVERE,
        "Could not fetch the feature store metadata for feature store: " +
          Hops.getProjectFeaturestore(), e);
    }
  }
  
  /**
   * Get the Avro schema for a particular Kafka topic.
   *
   * @param topic Kafka topic name.
   * @return Avro schema as String object in JSON format
   * @throws SchemaNotFoundException      SchemaNotFoundException
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   */
  public static String getSchema(String topic) throws SchemaNotFoundException, CredentialsNotFoundException {
    return getSchema(topic, Integer.MIN_VALUE);
  }

  /**
   * Get Avro Schemas for all Kafka topics directly using topics retrieved from Hopsworks.
   *
   * @param topics kafka topics.
   * @return Map of schemas.
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws SchemaNotFoundException      SchemaNotFoundException
   */
  public static Map<String, Schema> getSchemas(String[] topics) throws CredentialsNotFoundException,
    SchemaNotFoundException {
    if(topics == null) {
      throw new IllegalArgumentException("Topics were not provided.");
    }
    Map<String, Schema> schemas = new HashMap<>();
    for (String topic : topics) {
      Schema.Parser parser = new Schema.Parser();
      schemas.put(topic, parser.parse(getSchema(topic)));
    }
    return schemas;
  }

  /**
   * Get the Avro schema for a particular Kafka topic and its version.
   *
   * @param topic     Kafka topic name.
   * @param versionId Schema version ID
   * @return Avro schema as String object in JSON format
   * @throws SchemaNotFoundException      SchemaNotFoundException
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   */
  public static String getSchema(String topic, int versionId) throws CredentialsNotFoundException,
    SchemaNotFoundException {
    LOG.log(Level.FINE, "Getting schema for topic:{0}", new String[]{topic});

    JSONObject json = new JSONObject();
    json.append("topicName", topic);
    if (versionId > 0) {
      json.append("version", versionId);
    }
    Response response;
    try {
      response = clientWrapper(json, "schema", HttpMethod.POST);
    } catch (HTTPSClientInitializationException e) {
      throw new SchemaNotFoundException(e.getMessage());
    }
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      throw new SchemaNotFoundException("No schema found for topic:" + topic);
    }
    final String responseEntity = response.readEntity(String.class);
    //Extract fields from json
    json = new JSONObject(responseEntity);
    return json.getString("contents");
  }
  
  public static Properties getKafkaSSLProperties() {
    Properties properties = new Properties();
    properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    properties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, Hops.getTrustStore());
    properties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, Hops.getTruststorePwd());
    properties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, Hops.getKeyStore());
    properties.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, Hops.getKeystorePwd());
    properties.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, Hops.getKeystorePwd());
    return properties;
  }
  
  static Response clientWrapper(JSONObject json, String resource, String httpMethod)
      throws CredentialsNotFoundException,
      HTTPSClientInitializationException {
    json.append(Constants.JSON_KEYSTOREPWD, keystorePwd);
    try {
      json.append(Constants.JSON_KEYSTORE, keystoreEncode());
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, null, ex);
      throw new CredentialsNotFoundException("Could not initialize Hops properties.");
    }

    Client client;
    try {
      client = initClient();
    } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
      throw new HTTPSClientInitializationException(e.getMessage());
    }
    WebTarget webTarget = client.target(Hops.getRestEndpoint() + "/").path(Constants.HOPSWORKS_REST_RESOURCE
        + "/"
        + Constants.HOPSWORKS_REST_APPSERVICE
        + "/" + resource);
    LOG.info("webTarget.getUri().getHost():" + webTarget.getUri().getHost());
    LOG.info("webTarget.getUri().getPort():" + webTarget.getUri().getPort());
    LOG.info("webTarget.getUri().getPath():" + webTarget.getUri().getPath());
    Invocation.Builder invocationBuilder = webTarget.request().accept(MediaType.APPLICATION_JSON);
    if (httpMethod.equals(HttpMethod.PUT))
      return invocationBuilder.put(Entity.entity(json.toString(), MediaType.APPLICATION_JSON));
    else
      return invocationBuilder.post(Entity.entity(json.toString(), MediaType.APPLICATION_JSON));
  }


  /**
   * Get keystore password from local container.
   *
   * @return Certificate password.
   */
  private static String getCertPw() {
    try (FileInputStream fis = new FileInputStream(Constants.CRYPTO_MATERIAL_PASSWORD)) {
      StringBuilder sb = new StringBuilder();
      int content;
      while ((content = fis.read()) != -1) {
        sb.append((char) content);
      }
      return sb.toString();
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, null, ex);
    }
    return null;
  }

  /////////////////////////////////////////////
  private static String keystoreEncode() throws IOException {
    FileInputStream kfin = new FileInputStream(new File(keyStore));
    byte[] kStoreBlob = ByteStreams.toByteArray(kfin);
    return Base64.encodeBase64String(kStoreBlob);
  }

  /**
   * Get Kafka brokers endpoints as a List object.
   *
   * @return broker endpoints.
   */
  public static List<String> getBrokerEndpointsList() {
    return brokerEndpointsList;
  }

  /**
   * Get Kafka brokers endpoints as a String object.
   *
   * @return broker endpoints.
   */
  public static String getBrokerEndpoints() {
    return brokerEndpoints;
  }

  /**
   * Get Project ID of current job.
   *
   * @return HopsWorks project ID.
   */
  public static Integer getProjectId() {
    return projectId;
  }

  /**
   * Get REST Endpoint of Hopsworks.
   *
   * @return REST endpoint.
   */
  public static String getRestEndpoint() {
    return restEndpoint;
  }

  /**
   * Get keystore location.
   *
   * @return keystore location.
   */
  public static String getKeyStore() {
    return keyStore;
  }

  /**
   * Get truststore. location.
   *
   * @return truststore location
   */
  public static String getTrustStore() {
    return trustStore;
  }

  /**
   * Get keystore password.
   *
   * @return keystore password
   */
  public static String getKeystorePwd() {
    return keystorePwd;
  }

  /**
   * Get truststore password.
   *
   * @return truststore password.
   */
  public static String getTruststorePwd() {
    return truststorePwd;
  }

  /**
   * Get HopsWorks project name.
   *
   * @return project name.
   */
  public static String getProjectName() {
    return projectName;
  }

  /**
   * Get HopsWorks elasticsearch endpoint.
   *
   * @return elasticsearch endpoint.
   */
  public static String getElasticEndPoint() {
    return elasticEndPoint;
  }

  /**
   * Get HopsWorks job name.
   *
   * @return job name.
   */
  public static String getJobName() {
    return jobName;
  }

  /**
   * Get YARN applicationId for this job.
   *
   * @return applicationId.
   */
  public static String getAppId() {
    return appId;
  }

  /**
   * Get JobType.
   *
   * @return JobType.
   */
  public static String getJobType() {
    return jobType;
  }

  /**
   * Get WorkflowManager.
   *
   * @return WorkflowManager.
   */
  public static WorkflowManager getWorkflowManager() {
    return workflowManager;
  }

  /**
   * Populates the Kafka broker endpoints List with the value of the java system property passed by Hopsworks.
   *
   * @param addresses addresses
   */
  private static void parseBrokerEndpoints(String addresses) {
    brokerEndpoints = addresses;
    brokerEndpointsList = Arrays.asList(addresses.split(","));
  }

  private static Client initClient() throws KeyStoreException, IOException,
      NoSuchAlgorithmException,
      CertificateException {
    KeyStore truststore = KeyStore.getInstance(KeyStore.getDefaultType());

    try (FileInputStream trustStoreIS = new FileInputStream(Constants.DOMAIN_CA_TRUSTSTORE)) {
      truststore.load(trustStoreIS, null);
    }
    return ClientBuilder.newBuilder().trustStore(truststore).
        hostnameVerifier(InsecureHostnameVerifier.INSTANCE).build();
  }


  /**
   * Gets the project's featurestore name (project_featurestore)
   *
   * @return the featurestore name (hive db)
   */
  public static String getProjectFeaturestore() {
    return FeaturestoreHelper.getProjectFeaturestore();
  }

  /**
   * Makes a REST call to Hopsworks Appservice to get metadata about a featurestore, this metadata is then used by
   * hops-util to infer how to JOIN featuregroups together etc.
   *
   * @param featurestore the featurestore to query metadata about
   * @return a list of featuregroups metadata
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoresNotFound FeaturestoresNotFound
   * @throws JAXBException JAXBException
   */
  private static FeaturegroupsAndTrainingDatasetsDTO getFeaturestoreMetadataRest(String featurestore)
      throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    LOG.log(Level.FINE, "Getting featuregroups for featurestore " + featurestore);

    JSONObject json = new JSONObject();
    json.append(Constants.JSON_FEATURESTORE_NAME, featurestore);
    Response response = null;
    try {
      response = clientWrapper(json, Constants.HOPSWORKS_REST_APPSERVICE_FEATURESTORE_RESOURCE, HttpMethod.POST);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturestoreNotFound(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      throw new FeaturestoreNotFound("Could not fetch featuregroups for featurestore:" + featurestore);
    }
    final String responseEntity = response.readEntity(String.class);

    JSONObject featurestoreMetadata = new JSONObject(responseEntity);
    return FeaturestoreHelper.parseFeaturestoreMetadataJson(featurestoreMetadata);
  }

  /**
   * Makes a REST call to Hopsworks for deleting
   * the contents of the featuregroup but keeps the featuregroup metadata
   *
   * @param featurestore        the featurestore where the featuregroup resides
   * @param featuregroup        the featuregroup to drop the contents of
   * @param featuregroupVersion the version of the featurergroup
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoresNotFound FeaturestoresNotFound
   */
  private static void deleteTableContents(
      String featurestore, String featuregroup, int featuregroupVersion)
      throws CredentialsNotFoundException, FeaturegroupDeletionError {
    LOG.log(Level.FINE, "Deleting table contents of featuregroup " + featuregroup +
        "version: " + featuregroupVersion + " in featurestore: " + featurestore);

    JSONObject json = new JSONObject();
    json.append(Constants.JSON_FEATURESTORE_NAME, featurestore);
    json.append(Constants.JSON_FEATUREGROUP_NAME, featuregroup);
    json.append(Constants.JSON_FEATUREGROUP_VERSION, featuregroupVersion);
    try {
      Response response = clientWrapper(json, Constants.HOPSWORKS_REST_APPSERVICE_CLEAR_FEATUREGROUP_RESOURCE,
          HttpMethod.POST);
      LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
      if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
        throw new FeaturegroupDeletionError("Could not clear the contents of featuregroup:" + featuregroup);
      }
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupDeletionError(e.getMessage());
    }
  }

  /**
   * Makes a REST call to Hopsworks for creating a new featuregroup from a spark dataframe.
   *
   * @param featurestore        the featurestore where the group will be created
   * @param featuregroup        the name of the featuregroup
   * @param featuregroupVersion the version of the featuregroup
   * @param description         the description of the featuregroup
   * @param jobName               the name of the job to compute the featuregroup
   * @param dependencies        a list of dependencies (datasets that this featuregroup depends on)
   * @param featuresSchema      schema of features for the featuregroup
   * @param statisticsDTO       statistics about the featuregroup
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupCreationError FeaturegroupCreationError
   */
  private static void createFeaturegroupRest(
      String featurestore, String featuregroup, int featuregroupVersion, String description,
      String jobName, List<String> dependencies, List<FeatureDTO> featuresSchema,
    StatisticsDTO statisticsDTO)
      throws CredentialsNotFoundException, JAXBException, FeaturegroupCreationError {
    LOG.log(Level.FINE, "Creating featuregroup " + featuregroup +
        " in featurestore: " + featurestore);
    JSONObject json = new JSONObject();
    json.put(Constants.JSON_FEATURESTORE_NAME, featurestore);
    json.put(Constants.JSON_FEATUREGROUP_NAME, featuregroup);
    json.put(Constants.JSON_FEATUREGROUP_VERSION, featuregroupVersion);
    json.put(Constants.JSON_FEATUREGROUP_DESCRIPTION, description);
    json.put(Constants.JSON_FEATUREGROUP_JOBNAME, jobName);
    json.put(Constants.JSON_FEATUREGROUP_DEPENDENCIES, dependencies);
    json.put(Constants.JSON_FEATUREGROUP_FEATURES,
        FeaturestoreHelper.convertFeatureDTOsToJsonObjects(featuresSchema));
    json.put(Constants.JSON_FEATUREGROUP_FEATURE_CORRELATION,
        FeaturestoreHelper.convertFeatureCorrelationMatrixDTOToJsonObject(
            statisticsDTO.getFeatureCorrelationMatrixDTO()));
    json.put(Constants.JSON_FEATUREGROUP_DESC_STATS,
        FeaturestoreHelper.convertDescriptiveStatsDTOToJsonObject(statisticsDTO.getDescriptiveStatsDTO()));
    json.put(Constants.JSON_FEATUREGROUP_FEATURES_HISTOGRAM, FeaturestoreHelper
        .convertFeatureDistributionsDTOToJsonObject(statisticsDTO.getFeatureDistributionsDTO()));
    json.put(Constants.JSON_FEATUREGROUP_CLUSTER_ANALYSIS, FeaturestoreHelper
        .convertClusterAnalysisDTOToJsonObject(statisticsDTO.getClusterAnalysisDTO()));
    json.put(Constants.JSON_FEATUREGROUP_UPDATE_METADATA, false);
    json.put(Constants.JSON_FEATUREGROUP_UPDATE_STATS, false);
    Response response;
    try {
      response = clientWrapper(json, Constants.HOPSWORKS_REST_APPSERVICE_CREATE_FEATUREGROUP_RESOURCE, HttpMethod.POST);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupCreationError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.CREATED.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = parseHopsworksErrorResponse(response);
      throw new FeaturegroupCreationError("Could not create featuregroup:" + featuregroup +
          " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
          + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
  }

  /**
   * Makes a REST call to Hopsworks for creating a new training dataset from a spark dataframe
   *
   * @param featurestore           the featurestore where the group will be created
   * @param trainingDataset
   * @param trainingDatasetVersion the version of the featuregroup
   * @param description            the description of the featuregroup
   * @param jobName                  the name of the job to compute the featuregroup
   * @param dependencies           a list of dependencies (datasets that this featuregroup depends on)
   * @param featuresSchema         schema of features for the featuregroup
   * @param statisticsDTO          statistics about the featuregroup
   * @param dataFormat             format of the dataset (e.g tfrecords)
   * @return the JSON response
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws JAXBException JAXBException
   * @throws TrainingDatasetCreationError TrainingDatasetCreationError
   */
  private static Response createTrainingDatasetRest(
      String featurestore, String trainingDataset, int trainingDatasetVersion, String description,
      String jobName, String dataFormat, List<String> dependencies, List<FeatureDTO> featuresSchema,
      StatisticsDTO statisticsDTO) throws CredentialsNotFoundException, JAXBException, TrainingDatasetCreationError {
    LOG.log(Level.FINE, "Creating Training Dataset " + trainingDataset +
        " in featurestore: " + featurestore);
    JSONObject json = new JSONObject();
    json.put(Constants.JSON_FEATURESTORE_NAME, featurestore);
    json.put(Constants.JSON_TRAINING_DATASET_NAME, trainingDataset);
    json.put(Constants.JSON_TRAINING_DATASET_VERSION, trainingDatasetVersion);
    json.put(Constants.JSON_TRAINING_DATASET_DESCRIPTION, description);
    json.put(Constants.JSON_TRAINING_DATASET_JOBNAME, jobName);
    json.put(Constants.JSON_TRAINING_DATASET_DEPENDENCIES, dependencies);
    json.put(Constants.JSON_TRAINING_DATASET_FORMAT, dataFormat);
    json.put(Constants.JSON_TRAINING_DATASET_SCHEMA,
        FeaturestoreHelper.convertFeatureDTOsToJsonObjects(featuresSchema));
    json.put(Constants.JSON_TRAINING_DATASET_FEATURE_CORRELATION,
        FeaturestoreHelper.convertFeatureCorrelationMatrixDTOToJsonObject(
            statisticsDTO.getFeatureCorrelationMatrixDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_DESC_STATS,
        FeaturestoreHelper.convertDescriptiveStatsDTOToJsonObject(statisticsDTO.getDescriptiveStatsDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_FEATURES_HISTOGRAM, FeaturestoreHelper
        .convertFeatureDistributionsDTOToJsonObject(statisticsDTO.getFeatureDistributionsDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_CLUSTER_ANALYSIS, FeaturestoreHelper
        .convertClusterAnalysisDTOToJsonObject(statisticsDTO.getClusterAnalysisDTO()));
    Response response;
    try {
      response = clientWrapper(json,
          Constants.HOPSWORKS_REST_APPSERVICE_CREATE_TRAINING_DATASET_RESOURCE, HttpMethod.POST);
    } catch (HTTPSClientInitializationException e) {
      throw new TrainingDatasetCreationError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.CREATED.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = parseHopsworksErrorResponse(response);
      throw new TrainingDatasetCreationError("Could not create trainingDataset:" + trainingDataset +
          " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
          + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
    return response;
  }

  /**
   * Utility method for parsing the JSON response thrown by Hopsworks in case of errors
   *
   * @param response the JSON response to parse
   * @return a DTO with the parsed result
   */
  private static HopsworksErrorResponseDTO parseHopsworksErrorResponse(Response response) {
    String jsonStrResponse = response.readEntity(String.class);
    JSONObject jsonObjResponse = new JSONObject(jsonStrResponse);
    int errorCode = -1;
    String errorMsg = "";
    String userMsg = "";
    if (jsonObjResponse.has(Constants.JSON_ERROR_CODE))
      errorCode = jsonObjResponse.getInt(Constants.JSON_ERROR_CODE);
    if (jsonObjResponse.has(Constants.JSON_ERROR_MSG))
      errorMsg = jsonObjResponse.getString(Constants.JSON_ERROR_MSG);
    if (jsonObjResponse.has(Constants.JSON_USR_MSG))
      userMsg = jsonObjResponse.getString(Constants.JSON_USR_MSG);
    return new HopsworksErrorResponseDTO(errorCode, errorMsg, userMsg);
  }

  /**
   * Gets a list of featurestores accessible in the project (i.e the project's own featurestore
   * and the featurestores shared with the project)
   *
   * @return a list of names of the featurestores accessible by this project
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoresNotFound FeaturestoresNotFound
   */
  private static List<String> getFeaturestoresForProject()
      throws CredentialsNotFoundException, FeaturestoresNotFound {
    LOG.log(Level.FINE, "Getting featurestores for current project");

    JSONObject json = new JSONObject();
    Response response = null;
    try {
      response = clientWrapper(json, Constants.HOPSWORKS_REST_APPSERVICE_FEATURESTORES_RESOURCE, HttpMethod.POST);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturestoresNotFound(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      throw new FeaturestoresNotFound("Could not fetch featurestores for the current project");
    }
    final String responseEntity = response.readEntity(String.class);
    JSONArray featurestoresJson = new JSONArray(responseEntity);
    List<String> featurestores = new ArrayList();
    for (int i = 0; i < featurestoresJson.length(); i++) {
      JSONObject featurestoreJson = featurestoresJson.getJSONObject(i);
      String featurestoreName = featurestoreJson.getString(Constants.JSON_FEATURESTORE_NAME);
      featurestores.add(featurestoreName);
    }
    return featurestores;
  }

  /**
   * Inserts a spark dataframe into a featuregroup
   *
   * @param sparkDf             the spark dataframe to insert
   * @param sparkSession        the spark session
   * @param featuregroup        the name of the featuregroup to insert into
   * @param featurestore        the name of the featurestore where the featuregroup resides
   * @param featuregroupVersion the version of the featuregroup
   * @param mode                the mode to use when inserting (append/overwrite)
   * @param descriptiveStats    a boolean flag whether to compute descriptive statistics of the new data
   * @param featureCorr         a boolean flag whether to compute feature correlation analysis of the new data
   * @param featureHistograms   a boolean flag whether to compute feature histograms of the new data
   * @param clusterAnalysis     a boolean flag whether to compute cluster analysis of the new data
   * @param statColumns         a list of columns to compute statistics for (defaults to all columns that are numeric)
   * @param numBins             number of bins to use for computing histograms
   * @param corrMethod          the method to compute feature correlation with (pearson or spearman)
   * @param numClusters         number of clusters to use for cluster analysis
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturegroupDeletionError FeaturegroupDeletionError
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   */
  public static void insertIntoFeaturegroup(
      SparkSession sparkSession, Dataset<Row> sparkDf, String featuregroup,
      String featurestore, int featuregroupVersion, String mode, Boolean descriptiveStats, Boolean featureCorr,
      Boolean featureHistograms, Boolean clusterAnalysis, List<String> statColumns, Integer numBins,
      String corrMethod, Integer numClusters)
    throws CredentialsNotFoundException, FeaturegroupDeletionError, JAXBException, FeaturegroupUpdateStatsError,
    DataframeIsEmpty, SparkDataTypeNotRecognizedError, FeaturestoreNotFound {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    sparkSession = FeaturestoreHelper.sparkGetOrDefault(sparkSession);
    corrMethod = FeaturestoreHelper.correlationMethodGetOrDefault(corrMethod);
    numBins = FeaturestoreHelper.numBinsGetOrDefault(numBins);
    numClusters = FeaturestoreHelper.numClustersGetOrDefault(numClusters);
    sparkSession.sparkContext().setJobGroup(
        "Inserting dataframe into featuregroup",
        "Inserting into featuregroup:" + featuregroup + " in the featurestore:" +
            featurestore, true);
    if (!mode.equalsIgnoreCase("append") && !mode.equalsIgnoreCase("overwrite"))
      throw new IllegalArgumentException("The supplied write mode: " + mode +
          " does not match any of the supported modes: overwrite, append");
    if (mode.equalsIgnoreCase("overwrite")) {
      deleteTableContents(featurestore, featuregroup, featuregroupVersion);
    }
    sparkSession.sparkContext().setJobGroup("", "", true);
    FeaturestoreHelper.insertIntoFeaturegroup(sparkDf, sparkSession, featuregroup,
        featurestore, featuregroupVersion);
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(featuregroup, sparkSession, sparkDf,
        featurestore, featuregroupVersion,
        descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns, numBins, numClusters,
        corrMethod);
    updateFeaturegroupStatsRest(featuregroup, featurestore, featuregroupVersion, statisticsDTO);
  }

  /**
   * Inserts a spark dataframe into an existing training dataset (append/overwrite)
   *
   * @param sparkDf                the spark dataframe to insert
   * @param sparkSession           the spark session
   * @param trainingDataset        the name of the training dataset to insert into
   * @param featurestore           the name of the featurestore where the training dataset resides
   * @param trainingDatasetVersion the version of the training dataset
   * @param descriptiveStats       a boolean flag whether to compute descriptive statistics of the new data
   * @param featureCorr            a boolean flag whether to compute feature correlation analysis of the new data
   * @param featureHistograms      a boolean flag whether to compute feature histograms of the new data
   * @param clusterAnalysis        a boolean flag whether to compute cluster analysis of the new data
   * @param statColumns            a list of columns to compute statistics for (defaults to all columns
   *                               that are numeric)
   * @param numBins                number of bins to use for computing histograms
   * @param corrMethod             the method to compute feature correlation with (pearson or spearman)
   * @param numClusters            number of clusters to use for cluster analysis
   * @param writeMode              the spark write mode (append/overwrite)
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws CannotWriteImageDataFrameException CannotWriteImageDataFrameException
   */
  public static void insertIntoTrainingDataset(
    SparkSession sparkSession, Dataset<Row> sparkDf, String trainingDataset,
    String featurestore, int trainingDatasetVersion,
    Boolean descriptiveStats, Boolean featureCorr,
    Boolean featureHistograms, Boolean clusterAnalysis, List<String> statColumns, Integer numBins,
    String corrMethod, Integer numClusters, String writeMode)
    throws CredentialsNotFoundException, JAXBException, FeaturestoreNotFound,
    TrainingDatasetDoesNotExistError, DataframeIsEmpty, FeaturegroupUpdateStatsError,
    TrainingDatasetFormatNotSupportedError, SparkDataTypeNotRecognizedError, CannotWriteImageDataFrameException {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    try {
      doInsertIntoTrainingDataset(sparkSession, sparkDf, trainingDataset, featurestore,
        getFeaturestoreMetadata(featurestore), trainingDatasetVersion, descriptiveStats, featureCorr,
        featureHistograms, clusterAnalysis, statColumns, numBins, corrMethod, numClusters,
        writeMode);
    } catch (Exception e) {
      updateFeaturestoreMetadataCache(featurestore);
      doInsertIntoTrainingDataset(sparkSession, sparkDf, trainingDataset, featurestore,
        getFeaturestoreMetadata(featurestore), trainingDatasetVersion, descriptiveStats, featureCorr,
        featureHistograms, clusterAnalysis, statColumns, numBins, corrMethod, numClusters,
        writeMode);
    }
  }

  /**
   * Inserts a spark dataframe into an existing training dataset (append/overwrite)
   *
   * @param sparkDf                the spark dataframe to insert
   * @param sparkSession           the spark session
   * @param trainingDataset        the name of the training dataset to insert into
   * @param featurestore           the name of the featurestore where the training dataset resides
   * @param featurestoreMetadata   metadata of the featurestore to query
   * @param trainingDatasetVersion the version of the training dataset
   * @param descriptiveStats       a boolean flag whether to compute descriptive statistics of the new data
   * @param featureCorr            a boolean flag whether to compute feature correlation analysis of the new data
   * @param featureHistograms      a boolean flag whether to compute feature histograms of the new data
   * @param clusterAnalysis        a boolean flag whether to compute cluster analysis of the new data
   * @param statColumns            a list of columns to compute statistics for (defaults to all columns
   *                               that are numeric)
   * @param numBins                number of bins to use for computing histograms
   * @param corrMethod             the method to compute feature correlation with (pearson or spearman)
   * @param numClusters            number of clusters to use for cluster analysis
   * @param writeMode              the spark write mode (append/overwrite)
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws CannotWriteImageDataFrameException CannotWriteImageDataFrameException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   */
  private static void doInsertIntoTrainingDataset(
      SparkSession sparkSession, Dataset<Row> sparkDf, String trainingDataset,
      String featurestore, FeaturegroupsAndTrainingDatasetsDTO featurestoreMetadata, int trainingDatasetVersion,
      Boolean descriptiveStats, Boolean featureCorr,
      Boolean featureHistograms, Boolean clusterAnalysis, List<String> statColumns, Integer numBins,
      String corrMethod, Integer numClusters, String writeMode)
    throws CredentialsNotFoundException, JAXBException,
    TrainingDatasetDoesNotExistError, DataframeIsEmpty, FeaturegroupUpdateStatsError,
    TrainingDatasetFormatNotSupportedError, SparkDataTypeNotRecognizedError, CannotWriteImageDataFrameException,
    FeaturestoreNotFound {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    sparkSession = FeaturestoreHelper.sparkGetOrDefault(sparkSession);
    corrMethod = FeaturestoreHelper.correlationMethodGetOrDefault(corrMethod);
    numBins = FeaturestoreHelper.numBinsGetOrDefault(numBins);
    numClusters = FeaturestoreHelper.numClustersGetOrDefault(numClusters);
    List<TrainingDatasetDTO> trainingDatasetDTOList = featurestoreMetadata.getTrainingDatasets();
    FeaturestoreHelper.findTrainingDataset(trainingDatasetDTOList,
        trainingDataset, trainingDatasetVersion);
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(trainingDataset, sparkSession,
        sparkDf,
        featurestore, trainingDatasetVersion,
        descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns, numBins, numClusters,
        corrMethod);
    Response response = updateTrainingDatasetStatsRest(trainingDataset, featurestore, trainingDatasetVersion,
        statisticsDTO);
    String jsonStrResponse = response.readEntity(String.class);
    JSONObject jsonObjResponse = new JSONObject(jsonStrResponse);
    TrainingDatasetDTO updatedTrainingDatasetDTO = FeaturestoreHelper.parseTrainingDatasetJson(jsonObjResponse);
    String hdfsPath = updatedTrainingDatasetDTO.getHdfsStorePath() + "/" + trainingDataset;
    FeaturestoreHelper.writeTrainingDatasetHdfs(sparkSession, sparkDf, hdfsPath,
        updatedTrainingDatasetDTO.getDataFormat(), writeMode);
    if (updatedTrainingDatasetDTO.getDataFormat() == Constants.TRAINING_DATASET_TFRECORDS_FORMAT) {
      JSONObject tfRecordSchemaJson = null;
      try{
        tfRecordSchemaJson = FeaturestoreHelper.getDataframeTfRecordSchemaJson(sparkDf);
      } catch (Exception e){
        LOG.log(Level.WARNING, "Could not infer the TF-record schema for the training dataset");
      }
      if(tfRecordSchemaJson != null){
        try {
          FeaturestoreHelper.writeTfRecordSchemaJson(updatedTrainingDatasetDTO.getHdfsStorePath()
              + Constants.SLASH_DELIMITER + Constants.TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME,
            tfRecordSchemaJson.toString());
        } catch (Exception e) {
          LOG.log(Level.WARNING, "Could not save tf record schema json to HDFS for training dataset: "
            + trainingDataset, e);
        }
      }
    }
  }


  /**
   * Gets a featuregroup from a particular featurestore
   *
   * @param sparkSession        the spark session
   * @param featuregroup        the featuregroup to get
   * @param featurestore        the featurestore to query
   * @param featuregroupVersion the version of the featuregroup to get
   * @return a spark dataframe with the featuregroup
   */
  public static Dataset<Row> getFeaturegroup(SparkSession sparkSession, String featuregroup,
                                             String featurestore, int featuregroupVersion) {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    sparkSession = FeaturestoreHelper.sparkGetOrDefault(sparkSession);
    return FeaturestoreHelper.getFeaturegroup(sparkSession, featuregroup, featurestore, featuregroupVersion);
  }

  /**
   * Gets a training dataset from a featurestore
   *
   * @param sparkSession           the spark session
   * @param trainingDataset        the training dataset to get
   * @param featurestore           the featurestore where the training dataset resides
   * @param trainingDatasetVersion the version of the training dataset
   * @return a spark dataframe with the training dataset
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws IOException IOException
   */
  public static Dataset<Row> getTrainingDataset(
    SparkSession sparkSession, String trainingDataset,
    String featurestore, int trainingDatasetVersion)
    throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException, TrainingDatasetDoesNotExistError,
    TrainingDatasetFormatNotSupportedError, IOException {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    try {
      return doGetTrainingDataset(sparkSession, trainingDataset, getFeaturestoreMetadata(featurestore),
        trainingDatasetVersion);
    } catch(Exception e) {
      updateFeaturestoreMetadataCache(featurestore);
      return doGetTrainingDataset(sparkSession, trainingDataset, getFeaturestoreMetadata(featurestore),
        trainingDatasetVersion);
    }
  }

  /**
   * Gets a training dataset from a featurestore
   *
   * @param sparkSession           the spark session
   * @param trainingDataset        the training dataset to get
   * @param featurestoreMetadata   metadata of the featurestore to query
   * @param trainingDatasetVersion the version of the training dataset
   * @return a spark dataframe with the training dataset
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws IOException IOException
   */
  private static Dataset<Row> doGetTrainingDataset(
      SparkSession sparkSession, String trainingDataset,
    FeaturegroupsAndTrainingDatasetsDTO featurestoreMetadata, int trainingDatasetVersion)
      throws TrainingDatasetDoesNotExistError,
      TrainingDatasetFormatNotSupportedError, IOException {
    sparkSession = FeaturestoreHelper.sparkGetOrDefault(sparkSession);
    List<TrainingDatasetDTO> trainingDatasetDTOList = featurestoreMetadata.getTrainingDatasets();
    TrainingDatasetDTO trainingDatasetDTO = FeaturestoreHelper.findTrainingDataset(trainingDatasetDTOList,
        trainingDataset, trainingDatasetVersion);
    String hdfsPath = Constants.HDFS_DEFAULT + trainingDatasetDTO.getHdfsStorePath() +
        Constants.SLASH_DELIMITER + trainingDatasetDTO.getName();
    String dataFormat = trainingDatasetDTO.getDataFormat();
    if(dataFormat.equalsIgnoreCase(Constants.TRAINING_DATASET_IMAGE_FORMAT)){
      hdfsPath = Constants.HDFS_DEFAULT + trainingDatasetDTO.getHdfsStorePath();
    }
    return FeaturestoreHelper.getTrainingDataset(sparkSession, trainingDatasetDTO.getDataFormat(),
        hdfsPath);
  }

  /**
   * Gets a feature from a featurestore and infers the featuregroup where the feature is located
   *
   * @param sparkSession the spark session
   * @param feature      the feature to get
   * @param featurestore the featurestore to query
   * @return A dataframe with the feature
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static Dataset<Row> getFeature(
    SparkSession sparkSession, String feature,
    String featurestore) throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    try {
      return doGetFeature(sparkSession, feature, getFeaturestoreMetadata(featurestore), featurestore);
    } catch (Exception e) {
      updateFeaturestoreMetadataCache(featurestore);
      return doGetFeature(sparkSession, feature, getFeaturestoreMetadata(featurestore), featurestore);
    }
  }

  /**
   * Gets a feature from a featurestore and infers the featuregroup where the feature is located
   *
   * @param sparkSession the spark session
   * @param feature      the feature to get
   * @param featurestoreMetadata metadata of the featurestore to query
   * @param featurestore the featurestore to query
   * @return A dataframe with the feature
   */
  private static Dataset<Row> doGetFeature(
      SparkSession sparkSession, String feature,
    FeaturegroupsAndTrainingDatasetsDTO featurestoreMetadata, String featurestore) {
    sparkSession = FeaturestoreHelper.sparkGetOrDefault(sparkSession);
    List<FeaturegroupDTO> featuregroupsMetadata = featurestoreMetadata.getFeaturegroups();
    return FeaturestoreHelper.getFeature(sparkSession, feature, featurestore, featuregroupsMetadata);
  }

  /**
   * Gets a feature from a featurestore and a specific featuregroup.
   *
   * @param sparkSession        the spark session
   * @param feature             the feature to get
   * @param featurestore        the featurestore to query
   * @param featuregroup        the featuregroup where the feature is located
   * @param featuregroupVersion the version of the featuregroup
   * @return a spark dataframe with the feature
   */
  public static Dataset<Row> getFeature(SparkSession sparkSession, String feature, String featurestore,
                                        String featuregroup, int featuregroupVersion) {
    return FeaturestoreHelper.getFeature(sparkSession, feature, featurestore, featuregroup, featuregroupVersion);
  }

  /**
   * Method for updating the statistics of a featuregroup (recomputing the statistics)
   *
   * @param sparkSession        the spark session
   * @param featuregroup        the name of the featuregroup to update statistics for
   * @param featurestore        the name of the featurestore where the featuregroup resides
   * @param featuregroupVersion the version of the featuregroup
   * @param descriptiveStats    a boolean flag whether to compute descriptive statistics of the new data
   * @param featureCorr         a boolean flag whether to compute feature correlation analysis of the new data
   * @param featureHistograms   a boolean flag whether to compute feature histograms of the new data
   * @param clusterAnalysis     a boolean flag whether to compute cluster analysis of the new data
   * @param statColumns         a list of columns to compute statistics for (defaults to all columns that are numeric)
   * @param numBins             number of bins to use for computing histograms
   * @param corrMethod          the method to compute feature correlation with (pearson or spearman)
   * @param numClusters         number of clusters to use for cluster analysis
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   */
  public static void updateFeaturegroupStats(
      SparkSession sparkSession, String featuregroup, String featurestore,
      int featuregroupVersion, Boolean descriptiveStats, Boolean featureCorr,
      Boolean featureHistograms, Boolean clusterAnalysis, List<String> statColumns, Integer numBins,
      String corrMethod, Integer numClusters) throws DataframeIsEmpty, CredentialsNotFoundException, JAXBException,
    FeaturegroupUpdateStatsError, SparkDataTypeNotRecognizedError, FeaturestoreNotFound {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    sparkSession = FeaturestoreHelper.sparkGetOrDefault(sparkSession);
    corrMethod = FeaturestoreHelper.correlationMethodGetOrDefault(corrMethod);
    numBins = FeaturestoreHelper.numBinsGetOrDefault(numBins);
    numClusters = FeaturestoreHelper.numClustersGetOrDefault(numClusters);
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(featuregroup, sparkSession, null,
        featurestore, featuregroupVersion,
        descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns, numBins, numClusters,
        corrMethod);
    updateFeaturegroupStatsRest(featuregroup, featurestore, featuregroupVersion, statisticsDTO);
  }

  /**
   * Method for updating the statistics of a training dataset (recomputing the statistics)
   *
   * @param sparkSession           the spark session
   * @param trainingDataset        the name of the training datasaet to update statistics for
   * @param featurestore           the name of the featurestore where the featuregroup resides
   * @param trainingDatasetVersion the version of the training dataset
   * @param descriptiveStats       a boolean flag whether to compute descriptive statistics of the new data
   * @param featureCorr            a boolean flag whether to compute feature correlation analysis of the new data
   * @param featureHistograms      a boolean flag whether to compute feature histograms of the new data
   * @param clusterAnalysis        a boolean flag whether to compute cluster analysis of the new data
   * @param statColumns            a list of columns to compute statistics for (defaults to all
   *                               columns that are numeric)
   * @param numBins                number of bins to use for computing histograms
   * @param corrMethod             the method to compute feature correlation with (pearson or spearman)
   * @param numClusters            number of clusters to use for cluster analysis
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws JAXBException JAXBException
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws IOException IOException
   */
  public static void updateTrainingDatasetStats(
      SparkSession sparkSession, String trainingDataset, String featurestore,
      int trainingDatasetVersion, Boolean descriptiveStats, Boolean featureCorr,
      Boolean featureHistograms, Boolean clusterAnalysis, List<String> statColumns, Integer numBins,
      String corrMethod, Integer numClusters) throws DataframeIsEmpty, CredentialsNotFoundException, JAXBException,
      FeaturegroupUpdateStatsError, SparkDataTypeNotRecognizedError, TrainingDatasetFormatNotSupportedError,
      FeaturestoreNotFound, TrainingDatasetDoesNotExistError, IOException {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    sparkSession = FeaturestoreHelper.sparkGetOrDefault(sparkSession);
    corrMethod = FeaturestoreHelper.correlationMethodGetOrDefault(corrMethod);
    numBins = FeaturestoreHelper.numBinsGetOrDefault(numBins);
    numClusters = FeaturestoreHelper.numClustersGetOrDefault(numClusters);
    Dataset<Row> sparkDf = getTrainingDataset(sparkSession, trainingDataset, featurestore, trainingDatasetVersion);
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(trainingDataset, sparkSession, sparkDf,
        featurestore, trainingDatasetVersion,
        descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns, numBins, numClusters,
        corrMethod);
    updateTrainingDatasetStatsRest(trainingDataset, featurestore, trainingDatasetVersion, statisticsDTO);
  }

  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe. This method is used if the user
   * has itself provided a set of featuregroups where the features are located and should be queried from
   * and a join key, it does not infer the featuregroups.
   *
   * @param sparkSession             the spark session
   * @param features                 the list of features to get
   * @param featurestore             the featurestore to query
   * @param featuregroupsAndVersions a map of (featuregroup to version) where the featuregroups are located
   * @param joinKey                  the key to join on
   * @return a spark dataframe with the features
   */
  public static Dataset<Row> getFeatures(SparkSession sparkSession, List<String> features, String featurestore,
                                         Map<String, Integer> featuregroupsAndVersions, String joinKey) {
    return FeaturestoreHelper.getFeatures(sparkSession, features, featurestore, featuregroupsAndVersions, joinKey);
  }

  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe. This method is used if the user
   * has itself provided a set of featuregroups where the features are located and should be queried from
   * but not a join key, it does not infer the featuregroups but infers the join key
   *
   * @param sparkSession             the spark session
   * @param features                 the list of features to get
   * @param featurestore             the featurestore to query
   * @param featuregroupsAndVersions a map of (featuregroup to version) where the featuregroups are located
   * @return a spark dataframe with the features
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static Dataset<Row> getFeatures(
    SparkSession sparkSession, List<String> features, String featurestore,
    Map<String, Integer> featuregroupsAndVersions)
    throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    try {
      return doGetFeatures(sparkSession, features, featurestore, getFeaturestoreMetadata(featurestore),
        featuregroupsAndVersions);
    } catch(Exception e) {
      updateFeaturestoreMetadataCache(featurestore);
      return doGetFeatures(sparkSession, features, featurestore, getFeaturestoreMetadata(featurestore),
        featuregroupsAndVersions);
    }
  }

  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe. This method is used if the user
   * has itself provided a set of featuregroups where the features are located and should be queried from
   * but not a join key, it does not infer the featuregroups but infers the join key
   *
   * @param sparkSession             the spark session
   * @param features                 the list of features to get
   * @param featurestore             the featurestore to query
   * @param featurestoreMetadata     metadata of the featurestore to query
   * @param featuregroupsAndVersions a map of (featuregroup to version) where the featuregroups are located
   * @return a spark dataframe with the features
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  private static Dataset<Row> doGetFeatures(
      SparkSession sparkSession, List<String> features, String featurestore,
      FeaturegroupsAndTrainingDatasetsDTO featurestoreMetadata,
      Map<String, Integer> featuregroupsAndVersions) {
    List<FeaturegroupDTO> featuregroupsMetadata = featurestoreMetadata.getFeaturegroups();
    List<FeaturegroupDTO> filteredFeaturegroupsMetadata =
        FeaturestoreHelper.filterFeaturegroupsBasedOnMap(featuregroupsAndVersions, featuregroupsMetadata);
    String joinKey = FeaturestoreHelper.getJoinColumn(filteredFeaturegroupsMetadata);
    return FeaturestoreHelper.getFeatures(sparkSession, features, featurestore, featuregroupsAndVersions, joinKey);
  }

  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe. This method will infer
   * in which featuregroups the features belong but uses a user-supplied join key
   *
   * @param sparkSession the spark session
   * @param features     the list of features to get
   * @param featurestore the featurestore to query
   * @param joinKey      the key to join on
   * @return a spark dataframe with the features
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static Dataset<Row> getFeatures(
    SparkSession sparkSession, List<String> features,
    String featurestore, String joinKey) throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    try{
      return doGetFeatures(sparkSession, features, featurestore, getFeaturestoreMetadata(featurestore), joinKey);
    } catch(Exception e) {
      updateFeaturestoreMetadataCache(featurestore);
      return doGetFeatures(sparkSession, features, featurestore, getFeaturestoreMetadata(featurestore), joinKey);
    }
  }

  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe. This method will infer
   * in which featuregroups the features belong but uses a user-supplied join key
   *
   * @param sparkSession the spark session
   * @param features     the list of features to get
   * @param featurestore the featurestore to query
   * @param featurestoreMetadata metadata of the featurestore to query
   * @param joinKey      the key to join on
   * @return a spark dataframe with the features
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  private static Dataset<Row> doGetFeatures(
      SparkSession sparkSession, List<String> features,
      String featurestore, FeaturegroupsAndTrainingDatasetsDTO featurestoreMetadata, String joinKey) {
    List<FeaturegroupDTO> featuregroupsMetadata = featurestoreMetadata.getFeaturegroups();
    return FeaturestoreHelper.getFeatures(sparkSession, features, featurestore, featuregroupsMetadata, joinKey);
  }

  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe. This method will infer
   * in which featuregroups the features belong and which join_key to use using metadata from the metastore
   *
   * @param sparkSession the spark session
   * @param features     the list of features to get
   * @param featurestore the featurestore to query
   * @return a spark dataframe with the features
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static Dataset<Row> getFeatures(
    SparkSession sparkSession, List<String> features,
    String featurestore) throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    try {
      return doGetFeatures(sparkSession, features, featurestore, getFeaturestoreMetadata(featurestore));
    } catch (Exception e){
      updateFeaturestoreMetadataCache(featurestore);
      return doGetFeatures(sparkSession, features, featurestore, getFeaturestoreMetadata(featurestore));
    }
  }

  /**
   * Gets a set of features from a featurestore and returns them as a Spark dataframe. This method will infer
   * in which featuregroups the features belong and which join_key to use using metadata from the metastore
   *
   * @param sparkSession the spark session
   * @param features     the list of features to get
   * @param featurestore the featurestore to query
   * @return a spark dataframe with the features
   */
  private static Dataset<Row> doGetFeatures(
      SparkSession sparkSession, List<String> features,
      String featurestore, FeaturegroupsAndTrainingDatasetsDTO featurestoreMetadata) {
    List<FeaturegroupDTO> featuregroupsMetadata = featurestoreMetadata.getFeaturegroups();
    List<FeaturegroupDTO> featuregroupsMatching =
        FeaturestoreHelper.findFeaturegroupsThatContainsFeatures(featuregroupsMetadata, features, featurestore);
    String joinKey = FeaturestoreHelper.getJoinColumn(featuregroupsMatching);
    return FeaturestoreHelper.getFeatures(sparkSession, features, featurestore, featuregroupsMatching, joinKey);
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
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    return FeaturestoreHelper.queryFeaturestore(sparkSession, query, featurestore);
  }

  /**
   * Gets a list of featurestores accessible by the current project
   *
   * @return a list of names of the featurestores
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoresNotFound FeaturestoresNotFound
   */
  public static List<String> getProjectFeaturestores() throws CredentialsNotFoundException, FeaturestoresNotFound {
    return getFeaturestoresForProject();
  }

  /**
   * Gets a list of all featuregroups in a featurestore
   *
   * @param featurestore the featurestore to get the featuregroups for
   * @return a list of names of the feature groups
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static List<String> getFeaturegroups(String featurestore) throws
      CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    return getFeaturestoreMetadata(featurestore).
        getFeaturegroups().stream()
        .map(fg -> FeaturestoreHelper.getTableName(fg.getName(), fg.getVersion())).collect(Collectors.toList());
  }

  /**
   * Gets a list of all feature names in a featurestore
   *
   * @param featurestore the featurestore to get the features for
   * @return a list of names of the features in the feature store
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static List<String> getFeaturesList(String featurestore) throws
      CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    List<List<String>> featureNamesLists = getFeaturestoreMetadata(featurestore).
        getFeaturegroups().stream()
        .map(fg -> fg.getFeatures().stream().map(f -> f.getName())
            .collect(Collectors.toList())).collect(Collectors.toList());
    List<String> featureNames = new ArrayList<>();
    featureNamesLists.stream().forEach(flist -> featureNames.addAll(flist));
    return featureNames;
  }

  /**
   * Gets a list of all training datasets in a featurestore
   *
   * @param featurestore the featurestore to get the training datasets for
   * @return a list of names of the trainin datasets
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static List<String> getTrainingDatasets(String featurestore) throws CredentialsNotFoundException,
      FeaturestoreNotFound, JAXBException {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    return getFeaturestoreMetadata(featurestore).
        getTrainingDatasets().stream()
        .map(td -> FeaturestoreHelper.getTableName(td.getName(), td.getVersion())).collect(Collectors.toList());
  }

  /**
   * Gets the HDFS path to a training dataset with a specific name and version in a featurestore
   *
   * @param trainingDataset        name of the training dataset
   * @param featurestore           featurestore that the training dataset is linked to
   * @param trainingDatasetVersion version of the training dataset
   * @return the hdfs path to the training dataset
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static String getTrainingDatasetPath(String trainingDataset, String featurestore,
    int trainingDatasetVersion) throws
    TrainingDatasetDoesNotExistError, CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    try {
      return doGetTrainingDatasetPath(trainingDataset, trainingDatasetVersion,
        getFeaturestoreMetadata(featurestore));
    } catch (Exception e) {
      updateFeaturestoreMetadataCache(featurestore);
      return doGetTrainingDatasetPath(trainingDataset, trainingDatasetVersion,
        getFeaturestoreMetadata(featurestore));
    }
  }

  /**
   * Gets the HDFS path to a training dataset with a specific name and version in a featurestore
   *
   * @param trainingDataset        name of the training dataset
   * @param featurestoreMetadata   featurestore metadata
   * @param trainingDatasetVersion version of the training dataset
   * @return the hdfs path to the training dataset
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   */
  private static String doGetTrainingDatasetPath(String trainingDataset,
                                              int trainingDatasetVersion,
    FeaturegroupsAndTrainingDatasetsDTO featurestoreMetadata) throws
      TrainingDatasetDoesNotExistError {
    List<TrainingDatasetDTO> trainingDatasetDTOList = featurestoreMetadata.getTrainingDatasets();
    TrainingDatasetDTO trainingDatasetDTO = FeaturestoreHelper.findTrainingDataset(trainingDatasetDTOList,
        trainingDataset, trainingDatasetVersion);
    String hdfsPath = Constants.HDFS_DEFAULT + trainingDatasetDTO.getHdfsStorePath() +
        Constants.SLASH_DELIMITER + trainingDatasetDTO.getName();
    String dataFormat = trainingDatasetDTO.getDataFormat();
    if(dataFormat.equalsIgnoreCase(Constants.TRAINING_DATASET_IMAGE_FORMAT)){
      hdfsPath = Constants.HDFS_DEFAULT + trainingDatasetDTO.getHdfsStorePath();
    }
    return hdfsPath;
  }

  /**
   * Gets the metadata for the specified featurestore from the metastore
   *
   * @param featurestore the featurestore to query metadata from
   * @return a list of metadata about all the featuregroups in the featurestore
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static FeaturegroupsAndTrainingDatasetsDTO getFeaturestoreMetadata(String featurestore)
      throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    if(FeaturestoreHelper.getFeaturestoreMetadataCache() == null){
      updateFeaturestoreMetadataCache(featurestore);
    }
    return FeaturestoreHelper.getFeaturestoreMetadataCache();
  }

  /**
   * Updates the featurestore metadata cache
   *
   * @param featurestore the featurestore to update the metadata for
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static void updateFeaturestoreMetadataCache(String featurestore)
    throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    FeaturestoreHelper.setFeaturestoreMetadataCache(getFeaturestoreMetadataRest(featurestore));
  }

  /**
   * Makes a REST call to Hopsworks for updating the statistics of a featuregroup
   *
   * @param featuregroup        the name of the featuregroup
   * @param featurestore        the name of the featurestore where the featuregroup resides
   * @param featuregroupVersion the version of the featuregroup
   * @param statisticsDTO       the new statistics of the featuregroup
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   */
  public static void updateFeaturegroupStatsRest(
      String featuregroup, String featurestore, int featuregroupVersion, StatisticsDTO statisticsDTO)
      throws CredentialsNotFoundException,
      JAXBException, FeaturegroupUpdateStatsError {
    LOG.log(Level.FINE, "Updating featuregroup stats for: " + featuregroup +
        " in featurestore: " + featurestore);
    JSONObject json = new JSONObject();
    json.put(Constants.JSON_FEATURESTORE_NAME, featurestore);
    json.put(Constants.JSON_FEATUREGROUP_NAME, featuregroup);
    json.put(Constants.JSON_FEATUREGROUP_VERSION, featuregroupVersion);
    json.put(Constants.JSON_FEATUREGROUP_FEATURE_CORRELATION,
        FeaturestoreHelper.convertFeatureCorrelationMatrixDTOToJsonObject(
            statisticsDTO.getFeatureCorrelationMatrixDTO()));
    json.put(Constants.JSON_FEATUREGROUP_DESC_STATS,
        FeaturestoreHelper.convertDescriptiveStatsDTOToJsonObject(statisticsDTO.getDescriptiveStatsDTO()));
    json.put(Constants.JSON_FEATUREGROUP_FEATURES_HISTOGRAM, FeaturestoreHelper
        .convertFeatureDistributionsDTOToJsonObject(statisticsDTO.getFeatureDistributionsDTO()));
    json.put(Constants.JSON_FEATUREGROUP_CLUSTER_ANALYSIS, FeaturestoreHelper
        .convertClusterAnalysisDTOToJsonObject(statisticsDTO.getClusterAnalysisDTO()));
    json.put(Constants.JSON_FEATUREGROUP_UPDATE_METADATA, false);
    json.put(Constants.JSON_FEATUREGROUP_UPDATE_STATS, true);
    Response response = null;
    try {
      response = clientWrapper(json, Constants.HOPSWORKS_REST_APPSERVICE_UPDATE_FEATUREGROUP_RESOURCE, HttpMethod.PUT);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupUpdateStatsError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = parseHopsworksErrorResponse(response);
      throw new FeaturegroupUpdateStatsError("Could not update statistics for featuregroup:" + featuregroup +
          " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
          + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
  }

  /**
   * @param trainingDataset        the name of the training dataset
   * @param featurestore           the name of the featurestore where the training dataset resides
   * @param trainingDatasetVersion the version of the training dataset
   * @param statisticsDTO          the new statistics of the training dataset
   * @return the JSON response
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   */
  public static Response updateTrainingDatasetStatsRest(
      String trainingDataset, String featurestore, int trainingDatasetVersion, StatisticsDTO statisticsDTO)
      throws CredentialsNotFoundException,
      JAXBException, FeaturegroupUpdateStatsError {
    LOG.log(Level.FINE, "Updating training dataset stats for: " + trainingDataset +
        " in featurestore: " + featurestore);
    JSONObject json = new JSONObject();
    json.put(Constants.JSON_FEATURESTORE_NAME, featurestore);
    json.put(Constants.JSON_TRAINING_DATASET_NAME, trainingDataset);
    json.put(Constants.JSON_TRAINING_DATASET_VERSION, trainingDatasetVersion);
    json.put(Constants.JSON_TRAINING_DATASET_FEATURE_CORRELATION,
        FeaturestoreHelper.convertFeatureCorrelationMatrixDTOToJsonObject(
            statisticsDTO.getFeatureCorrelationMatrixDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_DESC_STATS,
        FeaturestoreHelper.convertDescriptiveStatsDTOToJsonObject(statisticsDTO.getDescriptiveStatsDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_FEATURES_HISTOGRAM, FeaturestoreHelper
        .convertFeatureDistributionsDTOToJsonObject(statisticsDTO.getFeatureDistributionsDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_CLUSTER_ANALYSIS, FeaturestoreHelper
        .convertClusterAnalysisDTOToJsonObject(statisticsDTO.getClusterAnalysisDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_UPDATE_METADATA, false);
    json.put(Constants.JSON_FEATUREGROUP_UPDATE_STATS, true);
    Response response = null;
    try {
      response = clientWrapper(json, Constants.HOPSWORKS_REST_APPSERVICE_UPDATE_TRAINING_DATASET_RESOURCE,
          HttpMethod.PUT);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupUpdateStatsError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = parseHopsworksErrorResponse(response);
      throw new FeaturegroupUpdateStatsError("Could not update statistics for trainingDataset:" + trainingDataset +
          " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
          + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
    return response;
  }

  /**
   * Creates a new featuregroup from a spark dataframe
   *
   * @param sparkSession        the spark session
   * @param featuregroupDf      the spark dataframe
   * @param featuregroup        the name of the featuregroup
   * @param featurestore        the featurestore of the featuregroup (defaults to the project's featurestore)
   * @param featuregroupVersion the version of the featuregroup (defaults to 1)
   * @param description         a description of the featuregroup
   * @param jobName             name of the job to compute the feature group
   * @param dependencies        list of the datasets that this featuregroup depends on (e.g input datasets to the
   *                            feature engineering job)
   * @param primaryKey          the primary key of the new featuregroup, if not specified, the first column in the
   *                            dataframe will be used as primary
   * @param descriptiveStats    a bolean flag whether to compute descriptive statistics (min,max,mean etc) for the
   *                            featuregroup
   * @param featureCorr         a boolean flag whether to compute a feature correlation matrix for the numeric
   *                            columns in the featuregroup
   * @param featureHistograms   a boolean flag whether to compute histograms for the numeric columns in the
   *                            featuregroup
   * @param clusterAnalysis     a boolean flag whether to compute cluster analysis for the numeric columns in the
   *                            featuregroup
   * @param statColumns         a list of columns to compute statistics for (defaults to all columns that are numeric)
   * @param numBins             number of bins to use for computing histograms
   * @param corrMethod          the method to compute feature correlation with (pearson or spearman)
   * @param numClusters         the number of clusters to use for cluster analysis
   * @throws InvalidPrimaryKeyForFeaturegroup InvalidPrimaryKeyForFeaturegroup
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws JAXBException JAXBException
   * @throws FeaturegroupCreationError FeaturegroupCreationError
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   */
  public static void createFeaturegroup(
      SparkSession sparkSession, Dataset<Row> featuregroupDf, String featuregroup, String featurestore,
      int featuregroupVersion, String description, String jobName,
      List<String> dependencies, String primaryKey, Boolean descriptiveStats, Boolean featureCorr,
      Boolean featureHistograms, Boolean clusterAnalysis, List<String> statColumns, Integer numBins,
      String corrMethod, Integer numClusters)
    throws InvalidPrimaryKeyForFeaturegroup,
    CredentialsNotFoundException, DataframeIsEmpty, JAXBException, FeaturegroupCreationError,
    SparkDataTypeNotRecognizedError, FeaturestoreNotFound {
    FeaturestoreHelper.validateMetadata(featuregroup, featuregroupDf.dtypes(), dependencies, description);
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    sparkSession = FeaturestoreHelper.sparkGetOrDefault(sparkSession);
    primaryKey = FeaturestoreHelper.primaryKeyGetOrDefault(primaryKey, featuregroupDf);
    corrMethod = FeaturestoreHelper.correlationMethodGetOrDefault(corrMethod);
    numBins = FeaturestoreHelper.numBinsGetOrDefault(numBins);
    numClusters = FeaturestoreHelper.numClustersGetOrDefault(numClusters);
    jobName = FeaturestoreHelper.jobNameGetOrDefault(jobName);
    FeaturestoreHelper.validatePrimaryKey(featuregroupDf, primaryKey);
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(featuregroup, sparkSession, featuregroupDf,
        featurestore, featuregroupVersion,
        descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns, numBins, numClusters,
        corrMethod);
    List<FeatureDTO> featuresSchema = FeaturestoreHelper.parseSparkFeaturesSchema(featuregroupDf.schema(), primaryKey);
    createFeaturegroupRest(featurestore, featuregroup, featuregroupVersion, description,
        jobName, dependencies, featuresSchema, statisticsDTO);
    FeaturestoreHelper.insertIntoFeaturegroup(featuregroupDf, sparkSession, featuregroup,
        featurestore, featuregroupVersion);
    //Update metadata cache since we created a new feature group
    updateFeaturestoreMetadataCache(featurestore);
  }

  /**
   * Creates a new training dataset from a spark dataframe, saves metadata about the training dataset to the database
   * and saves the materialized dataset on hdfs
   *
   * @param sparkSession           the spark session
   * @param trainingDatasetDf      the spark dataframe to create the training dataset from
   * @param trainingDataset        the name of the training dataset
   * @param featurestore           the featurestore that the training dataset is linked to
   * @param trainingDatasetVersion the version of the training dataset (defaults to 1)
   * @param description            a description of the training dataset
   * @param jobName                the name of the job to compute the training dataset
   * @param dataFormat             the format of the materialized training dataset
   * @param dependencies           list of the datasets that this training dataset depends on
   *                               (e.g input datasets to the feature engineering job)
   * @param descriptiveStats       a bolean flag whether to compute descriptive statistics
   *                               (min,max,mean etc) for the featuregroup
   * @param featureCorr            a boolean flag whether to compute a feature correlation matrix for the
   *                               numeric columns in the featuregroup
   * @param featureHistograms      a boolean flag whether to compute histograms for the numeric columns in the
   *                               featuregroup
   * @param clusterAnalysis        a boolean flag whether to compute cluster analysis for the numeric columns in the
   *                               featuregroup
   * @param statColumns            a list of columns to compute statistics for (defaults to all columns
   *                               that are numeric)
   * @param numBins                number of bins to use for computing histograms
   * @param corrMethod             the method to compute feature correlation with (pearson or spearman)
   * @param numClusters            number of clusters to use for cluster analysis
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws DataframeIsEmpty DataframeIsEmpty
   * @throws JAXBException JAXBException
   * @throws TrainingDatasetCreationError TrainingDatasetCreationError
   * @throws TrainingDatasetFormatNotSupportedError TrainingDatasetFormatNotSupportedError
   * @throws SparkDataTypeNotRecognizedError SparkDataTypeNotRecognizedError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws CannotWriteImageDataFrameException CannotWriteImageDataFrameException
   */
  public static void createTrainingDataset(
      SparkSession sparkSession, Dataset<Row> trainingDatasetDf, String trainingDataset, String featurestore,
      int trainingDatasetVersion, String description, String jobName, String dataFormat,
      List<String> dependencies, Boolean descriptiveStats, Boolean featureCorr,
      Boolean featureHistograms, Boolean clusterAnalysis, List<String> statColumns, Integer numBins,
      String corrMethod, Integer numClusters)
    throws CredentialsNotFoundException, DataframeIsEmpty, JAXBException, TrainingDatasetCreationError,
    TrainingDatasetFormatNotSupportedError, SparkDataTypeNotRecognizedError, CannotWriteImageDataFrameException,
    FeaturestoreNotFound {
    FeaturestoreHelper.validateMetadata(trainingDataset, trainingDatasetDf.dtypes(), dependencies, description);
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    sparkSession = FeaturestoreHelper.sparkGetOrDefault(sparkSession);
    dataFormat = FeaturestoreHelper.dataFormatGetOrDefault(dataFormat);
    corrMethod = FeaturestoreHelper.correlationMethodGetOrDefault(corrMethod);
    numBins = FeaturestoreHelper.numBinsGetOrDefault(numBins);
    numClusters = FeaturestoreHelper.numClustersGetOrDefault(numClusters);
    jobName = FeaturestoreHelper.jobNameGetOrDefault(jobName);
    StatisticsDTO statisticsDTO = FeaturestoreHelper.computeDataFrameStats(trainingDataset, sparkSession,
        trainingDatasetDf, featurestore, trainingDatasetVersion,
        descriptiveStats, featureCorr, featureHistograms, clusterAnalysis, statColumns, numBins, numClusters,
        corrMethod);
    List<FeatureDTO> featuresSchema = FeaturestoreHelper.parseSparkFeaturesSchema(trainingDatasetDf.schema(),
        null);
    Response response = createTrainingDatasetRest(featurestore, trainingDataset, trainingDatasetVersion, description,
        jobName, dataFormat, dependencies, featuresSchema, statisticsDTO);
    String jsonStrResponse = response.readEntity(String.class);
    JSONObject jsonObjResponse = new JSONObject(jsonStrResponse);
    TrainingDatasetDTO trainingDatasetDTO = FeaturestoreHelper.parseTrainingDatasetJson(jsonObjResponse);
    String hdfsPath = trainingDatasetDTO.getHdfsStorePath() + Constants.SLASH_DELIMITER + trainingDataset;
    FeaturestoreHelper.writeTrainingDatasetHdfs(
        sparkSession, trainingDatasetDf, hdfsPath, dataFormat, Constants.SPARK_OVERWRITE_MODE);
    if (dataFormat == Constants.TRAINING_DATASET_TFRECORDS_FORMAT) {
      try {
        JSONObject tfRecordSchemaJson = FeaturestoreHelper.getDataframeTfRecordSchemaJson(trainingDatasetDf);
        FeaturestoreHelper.writeTfRecordSchemaJson(trainingDatasetDTO.getHdfsStorePath()
                + Constants.SLASH_DELIMITER + Constants.TRAINING_DATASET_TF_RECORD_SCHEMA_FILE_NAME,
            tfRecordSchemaJson.toString());
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Could not save tf record schema json to HDFS for training dataset: "
            + trainingDataset, e);
      }
    }
    //Update metadata cache since we created a new feature group
    updateFeaturestoreMetadataCache(featurestore);
  }

  /**
   * Gets the latest version of a feature group in the feature store, returns 0 if no version exists
   *
   * @param featuregroupName the name of the featuregroup to get the latest version of
   * @param featurestore     the featurestore where the featuregroup resides
   * @return the latest version of the feature group
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws  FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static int getLatestFeaturegroupVersion(
    String featuregroupName, String featurestore)
    throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    try {
      return doGetLatestFeaturegroupVersion(featuregroupName, getFeaturestoreMetadata(featurestore));
    } catch (Exception e) {
      updateFeaturestoreMetadataCache(featurestore);
      return doGetLatestFeaturegroupVersion(featuregroupName, getFeaturestoreMetadata(featurestore));
    }
  }

  /**
   * Gets the latest version of a feature group in the feature store, returns 0 if no version exists
   *
   * @param featuregroupName the name of the featuregroup to get the latest version of
   * @param featurestoreMetadata the featurestore where the featuregroup resides
   * @return the latest version of the feature group
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws  FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  private static int doGetLatestFeaturegroupVersion(
      String featuregroupName, FeaturegroupsAndTrainingDatasetsDTO featurestoreMetadata) {
    List<FeaturegroupDTO> featuregroupDTOList = featurestoreMetadata.getFeaturegroups();
    return FeaturestoreHelper.getLatestFeaturegroupVersion(featuregroupDTOList, featuregroupName);
  }

  /**
   * Gets the latest version of a training dataset in the feature store, returns 0 if no version exists
   *
   * @param trainingDatasetName the name of the trainingDataset to get the latest version of
   * @param featurestore        the featurestore where the training dataset resides
   * @return the latest version of the training dataset
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   */
  public static int getLatestTrainingDatasetVersion(
    String trainingDatasetName, String featurestore)
    throws CredentialsNotFoundException, FeaturestoreNotFound, JAXBException {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    try {
      return doGetLatestTrainingDatasetVersion(trainingDatasetName, getFeaturestoreMetadata(featurestore));
    } catch (Exception e) {
      updateFeaturestoreMetadataCache(featurestore);
      return doGetLatestTrainingDatasetVersion(trainingDatasetName, getFeaturestoreMetadata(featurestore));
    }
  }

  /**
   * Gets the latest version of a training dataset in the feature store, returns 0 if no version exists
   *
   * @param trainingDatasetName the name of the trainingDataset to get the latest version of
   * @param featurestoreMetadata metadata of the featurestore to query
   * @return the latest version of the training dataset
   */
  private static int doGetLatestTrainingDatasetVersion(
      String trainingDatasetName, FeaturegroupsAndTrainingDatasetsDTO featurestoreMetadata) {
    List<TrainingDatasetDTO> trainingDatasetDTOS = featurestoreMetadata.getTrainingDatasets();
    return FeaturestoreHelper.getLatestTrainingDatasetVersion(trainingDatasetDTOS, trainingDatasetName);
  }

  /**
   * Finds the spark session dynamically if it is not provided by the user-request
   *
   * @return spark session
   */
  public static SparkSession findSpark() {
    return SparkSession.builder().getOrCreate();
  }

  private static class InsecureHostnameVerifier implements HostnameVerifier {

    static InsecureHostnameVerifier INSTANCE = new InsecureHostnameVerifier();

    InsecureHostnameVerifier() {
    }

    @Override
    public boolean verify(String string, SSLSession ssls) {
      return string.equals(restEndpoint.split(":")[0]);
    }
  }

}
