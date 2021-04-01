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

import io.hops.util.exceptions.ElasticAuthorizationTokenException;
import io.hops.util.exceptions.HTTPSClientInitializationException;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.SchemaNotFoundException;
import org.apache.avro.Schema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

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
  private static boolean insecure;
  private static String keyStore;
  private static String trustStore;
  private static String keystorePwd;
  private static String truststorePwd;
  private static String elasticEndPoint;
  private static String domainCaTruststore;

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
    if (sysProps.containsKey(Constants.JOBTYPE_ENV_VAR) && sysProps.getProperty(Constants.JOBTYPE_ENV_VAR).
      equalsIgnoreCase("spark")) {
      restEndpoint = sysProps.getProperty(Constants.HOPSWORKS_RESTENDPOINT);
      insecure = Boolean.parseBoolean(sysProps.getProperty(Constants.HOPSUTIL_INSECURE));
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
      domainCaTruststore = sysProps.getProperty(Constants.DOMAIN_CA_TRUSTSTORE);
      //Spark Kafka topics
      if (sysProps.containsKey(Constants.KAFKA_BROKERADDR_ENV_VAR)) {
        parseBrokerEndpoints(sysProps.getProperty(Constants.KAFKA_BROKERADDR_ENV_VAR));
      }
    }
  }

  /**
   * Get Avro Schemas for all Kafka topics directly using topics retrieved from Hopsworks.
   *
   * @param topics kafka topics.
   * @return Map of schemas.
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws SchemaNotFoundException      SchemaNotFoundException
   */
  public static Map<String, Schema> getSchemas(String[] topics) throws JWTNotFoundException,
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
   * @return Avro schema as String object in JSON format
   * @throws SchemaNotFoundException      SchemaNotFoundException
   * @throws JWTNotFoundException JWTNotFoundException
   */
  public static String getSchema(String topic) throws
    JWTNotFoundException, SchemaNotFoundException {
    LOG.log(Level.FINE, "Getting schema for topic:{0}", new String[]{topic});

    JSONObject json = new JSONObject();
    json.append("topicName", topic);
    Response response = null;
    try {
      response = clientWrapper(json, "/project/" + projectId + "/kafka/topics/" + topic + "/subjects",
          HttpMethod.GET, null);
    } catch (HTTPSClientInitializationException e) {
      throw new SchemaNotFoundException(e.getMessage());
    }
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      throw new SchemaNotFoundException("No schema found for topic:" + topic);
    }
    final String responseEntity = response.readEntity(String.class);
    //Extract fields from json
    LOG.log(Level.FINE, "responseEntity:" + responseEntity);
    json = new JSONObject(responseEntity);
    return json.getString("schema");
  }

  public static Properties getKafkaSSLProperties() {
    Properties properties = new Properties();
    properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    properties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, Hops.getTrustStore());
    properties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, Hops.getTruststorePwd());
    properties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, Hops.getKeyStore());
    properties.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, Hops.getKeystorePwd());
    properties.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, Hops.getKeystorePwd());
    properties.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    return properties;
  }

  protected static Response clientWrapper(String path, String httpMethod, Map<String, Object> queryParams)
      throws HTTPSClientInitializationException, JWTNotFoundException {
    return clientWrapper(null, path, httpMethod, queryParams);
  }
  protected static Response clientWrapper(
      JSONObject json, String path, String httpMethod, Map<String, Object> queryParams) throws
      HTTPSClientInitializationException, JWTNotFoundException {
    Client client;
    try {
      client = initClient();
    } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
      throw new HTTPSClientInitializationException("Could not retrieve credentials from local working directory", e);
    }
    WebTarget webTarget = client.target(Hops.getRestEndpoint() + "/").path(Constants.HOPSWORKS_REST_RESOURCE + path);
    if(queryParams!= null && !queryParams.isEmpty()){
      for (Map.Entry<String, Object> entry : queryParams.entrySet()) {
        webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
      }
    }
    LOG.log(Level.FINE, "webTarget.getUri().getHost():" + webTarget.getUri().getHost());
    LOG.log(Level.FINE, "webTarget.getUri().getPort():" + webTarget.getUri().getPort());
    LOG.log(Level.FINE, "webTarget.getUri().getPath():" + webTarget.getUri().getPath());
    //Read jwt and set it in header
    Invocation.Builder invocationBuilder =
      webTarget.request().header(HttpHeaders.AUTHORIZATION,
        "Bearer " + getJwt().orElseThrow(IllegalArgumentException::new)).accept(MediaType.APPLICATION_JSON);

    switch (httpMethod) {
      case HttpMethod.PUT:
        if (json == null) {
          //put request with empty body
          return invocationBuilder.put(Entity.json(""));
        }
        return invocationBuilder.put(Entity.entity(json.toString(), MediaType.APPLICATION_JSON));
      case HttpMethod.POST:
        if (json == null) {
          //post request with empty body
          return invocationBuilder.post(Entity.json(""));
        }
        return invocationBuilder.post(Entity.entity(json.toString(), MediaType.APPLICATION_JSON));
      case HttpMethod.GET:
        return invocationBuilder.get();
      case HttpMethod.DELETE:
        return invocationBuilder.delete();
      default:
        break;
    }
    return null;
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

  private static synchronized Optional<String> getJwt() throws JWTNotFoundException {
    String jwt = null;
    try (FileChannel fc = FileChannel.open(Paths.get(Constants.JWT_FILENAME), StandardOpenOption.READ)) {
      FileLock fileLock = fc.tryLock(0, Long.MAX_VALUE, true);
      try {
        short numRetries = 5;
        short retries = 0;
        while (fileLock == null && retries < numRetries) {
          LOG.log(Level.FINEST, "Waiting for lock on jwt file at:" + Constants.JWT_FILENAME);
          Thread.sleep(1000);
          fileLock = fc.tryLock(0, Long.MAX_VALUE, true);
          retries++;
        }
        //If could not acquire lock in reasonable time, throw exception
        if (fileLock == null) {
          throw new JWTNotFoundException("Could not read jwt token from local container, possibly another process has" +
            " acquired the lock");
        }
        ByteBuffer buf = ByteBuffer.allocateDirect(512);
        fc.read(buf);
        buf.flip();
        jwt = StandardCharsets.UTF_8.decode(buf).toString();
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "JWT waiting thread was interrupted.", e);
      } finally {
        if (fileLock != null) {
          fileLock.release();
        }
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE,"Could not read jwt token from local container.", e);
      throw new JWTNotFoundException("Could not read jwt token from local container." + e.getMessage(), e);
    }
    return Optional.ofNullable(jwt);
  }

  /////////////////////////////////////////////

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
   * Populates the Kafka broker endpoints List with the value of the java system property passed by Hopsworks.
   *
   * @param addresses addresses
   */
  private static void parseBrokerEndpoints(String addresses) {
    brokerEndpoints = addresses;
    brokerEndpointsList = Arrays.asList(addresses.split(","));
  }

  private static Client initClient() throws KeyStoreException, IOException, NoSuchAlgorithmException,
      CertificateException {
    KeyStore truststore = KeyStore.getInstance(KeyStore.getDefaultType());

    try (FileInputStream trustStoreIS = new FileInputStream(domainCaTruststore)) {
      truststore.load(trustStoreIS, null);
    }
    return ClientBuilder.newBuilder().trustStore(truststore).
        hostnameVerifier(InsecureHostnameVerifier.INSTANCE).build();
  }

  /**
   * Finds the spark session dynamically if it is not provided by the user-request
   *
   * @return spark session
   */
  public static SparkSession findSpark() {
    return SparkSession.builder().enableHiveSupport().getOrCreate();
  }

  private static class InsecureHostnameVerifier implements HostnameVerifier {

    static InsecureHostnameVerifier INSTANCE = new InsecureHostnameVerifier();

    InsecureHostnameVerifier() {
    }

    @Override
    public boolean verify(String string, SSLSession ssls) {
      return insecure || string.equals(restEndpoint.split(":")[0]);
    }
  }

  /**
   * Get a valid elastic index name for the current project.
   * @param index index to get name for
   * @return Elastic index name
   */
  public static String getElasticIndex(String index){
    return getProjectName() + "_" + index;
  }
  
  /**
   * Generate a new jwt token to be used with Elastic.
   * @return elastic auth token
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws ElasticAuthorizationTokenException ElasticAuthorizationTokenException
   */
  public static String getElasticAuthorizationToken()
      throws JWTNotFoundException, ElasticAuthorizationTokenException {
    Response response;
    try {
      response =
          clientWrapper(Constants.SLASH_DELIMITER
                  + Constants.HOPSWORKS_REST_ELASTIC_RESOURCE
                  + Constants.SLASH_DELIMITER
                  + Constants.HOPSWORKS_REST_JWT_RESOURCE
                  + Constants.SLASH_DELIMITER
                  + Hops.getProjectId(),
          HttpMethod.GET, null);
    } catch (HTTPSClientInitializationException e) {
      throw new ElasticAuthorizationTokenException(e.getMessage());
    }
    final String responseEntity = response.readEntity(String.class);
    
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      throw new ElasticAuthorizationTokenException(responseEntity);
    }
    
  
    JSONObject jsonResponse = new JSONObject(responseEntity);
    if(!jsonResponse.has("token"))
      throw new ElasticAuthorizationTokenException("Couldn't get " +
          "authorization token for elastic.");
    
    String token = jsonResponse.getString("token");
    if(token.isEmpty())
      throw new ElasticAuthorizationTokenException("Couldn't get " +
          "authorization token for elastic.");
    
    return "Bearer " + token;
  }
  
  /**
   * Get Elasticsearch configuration to use with spark connector.
   * @param index index
   * @return elasticsearch configurations
   * @throws ElasticAuthorizationTokenException ElasticAuthorizationTokenException
   * @throws JWTNotFoundException JWTNotFoundException
   */
  public static Map<String, String> getElasticConfiguration(String index)
      throws ElasticAuthorizationTokenException, JWTNotFoundException {
    Map<String, String> configs = new HashMap<>();
    configs.put("es.net.ssl","true");
    configs.put("es.nodes.wan.only", "true");
    configs.put("es.nodes", getElasticEndPoint());
    configs.put("es.net.ssl.keystore.location", getKeyStore());
    configs.put("es.net.ssl.keystore.pass", getKeystorePwd());
    configs.put("es.net.ssl.truststore.location", getTrustStore());
    configs.put("es.net.ssl.truststore.pass", getTruststorePwd());
    configs.put("es.net.http.header.Authorization", getElasticAuthorizationToken());
    configs.put("es.resource", getElasticIndex(index));
    return configs;
  }
  
  /**
   * Set environment variables
   * @param key
   * @param value
   */
  public static void setEnv(String key, String value) {
    try {
      Map<String, String> env = System.getenv();
      Class<?> cl = env.getClass();
      Field field = cl.getDeclaredField("m");
      field.setAccessible(true);
      Map<String, String> writableEnv = (Map<String, String>) field.get(env);
      writableEnv.put(key, value);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to set environment variable", e);
    }
  }
}
