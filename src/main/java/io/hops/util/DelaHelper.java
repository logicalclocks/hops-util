package io.hops.util;

import io.hops.util.exceptions.SchemaNotFoundException;
import com.google.common.io.ByteStreams;

import io.hops.util.dela.DelaConsumer;
import io.hops.util.dela.DelaProducer;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.avro.Schema;
import org.apache.commons.net.util.Base64;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.filter.LoggingFilter;
import org.json.JSONObject;

/**
 *
 * 
 */
public class DelaHelper {

  private static final Logger LOGGER = Logger.getLogger(DelaHelper.class.getName());

  /**
   *
   * @param projectId HopsWorks project ID.
   * @param topicName Kafka topic name.
   * @param brokerEndpoint Kafka broker address.
   * @param restEndpoint HopsWorks REST API endpoint.
   * @param keyStore keystore location.
   * @param trustStore truststore location.
   * @param keystorePwd keystore password.
   * @param truststorePwd truststore password.
   * @return DelaConsumer
   * @throws SchemaNotFoundException When Avro schema for topic could not be found in HopsWorks.
   */
  public static DelaConsumer getHopsConsumer(int projectId, String topicName, String brokerEndpoint,
      String restEndpoint, String keyStore, String trustStore, String keystorePwd, String truststorePwd)
      throws SchemaNotFoundException {
    LOGGER.log(Level.INFO, "projectId:{0}, topicName:{1}, brokerE:{2}, restE:{3}, keyS:{4}, trustS:{5}, keysP:{6}, "
        + "trustsP:{7}", new Object[]{projectId, topicName, brokerEndpoint, restEndpoint, keyStore, trustStore,
          keystorePwd, truststorePwd});
    Hops.setup(projectId, topicName, brokerEndpoint, restEndpoint, keyStore, trustStore, keystorePwd,
        truststorePwd);
    String stringSchema = getSchemaByTopic(Hops.getRestEndpoint(), projectId, topicName);
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(stringSchema);
    return new DelaConsumer(topicName, schema);
  }

  /**
   *
   * @param projectId HopsWorks project ID
   * @param topicName Kafka topic name
   * @param brokerEndpoint Kafka broker address
   * @param restEndpoint HopsWorks REST API endpoint
   * @param keyStore keystore location
   * @param trustStore truststore location
   * @param keystorePwd keystore password
   * @param truststorePwd truststore password
   * @param lingerDelay lingerDelay
   * @return DelaProducer
   * @throws SchemaNotFoundException When Avro schema for topic could not be found in HopsWorks.
   */
  public static DelaProducer getHopsProducer(int projectId, String topicName, String brokerEndpoint,
      String restEndpoint, String keyStore, String trustStore, String keystorePwd, String truststorePwd,
      long lingerDelay)
      throws SchemaNotFoundException {
    LOGGER.log(Level.INFO, "projectId:{0}, topicName:{1}, brokerE:{2}, restE:{3}, keyS:{4}, trustS:{5}, keysP:{6}, "
        + "trustsP:{7}, linger:{8}", new Object[]{projectId, topicName, brokerEndpoint, restEndpoint, keyStore,
          trustStore,
          keystorePwd, truststorePwd, lingerDelay});
    Hops.setup(projectId, topicName, brokerEndpoint, restEndpoint, keyStore, trustStore, keystorePwd,
        truststorePwd);
    String stringSchema = getSchemaByTopic(Hops.getRestEndpoint(), projectId, topicName);
    LOGGER.log(Level.INFO, "schema:{0}", new Object[]{stringSchema});
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(stringSchema);
    return new DelaProducer(topicName, schema, lingerDelay);
  }

  /**
   *
   * @param restEndpoint HopsWorks REST API endpoint
   * @param projectId projectId
   * @param topicName Kafka topic name
   * @return Avro schema as a string in JSON format
   * @throws SchemaNotFoundException When Avro schema for topic could not be found in HopsWorks.
   */
  public static String getSchemaByTopic(String restEndpoint, int projectId, String topicName)
      throws SchemaNotFoundException {
    return getSchemaByTopic(restEndpoint, projectId, topicName, Integer.MIN_VALUE);
  }

  /**
   *
   * @param restEndpoint HopsWorks REST API endpoint
   * @param projectId HopsWorks project ID
   * @param topicName Kafka topic name
   * @param versionId Avro schema version ID
   * @return Avro schema as a string object in JSON format
   * @throws SchemaNotFoundException When Avro schema for topic could not be found in HopsWorks.
   */
  public static String getSchemaByTopic(String restEndpoint, int projectId, String topicName, int versionId)
      throws SchemaNotFoundException {

    JSONObject json = new JSONObject();
    jsonAddKeyStore(json, Hops.getKeyStore(), Hops.getKeystorePwd());
    json.append(Constants.JSON_SCHEMA_TOPICNAME, topicName);
    if (versionId > 0) {
      json.append(Constants.JSON_SCHEMA_VERSION, versionId);
    }

    String uri = Hops.getRestEndpoint() + "/schema";
    LOGGER.log(Level.INFO, "getting schema:{0}", uri);
    Response response = postJsonResponse(uri, json.toString());
    if (response == null) {
      throw new SchemaNotFoundException("Could not reach schema endpoint");
    } else if (response.getStatus() != 200) {
      LOGGER.log(Level.WARNING, "error code:{0}", response.getStatus());
      LOGGER.log(Level.WARNING, "response:{0}", response.toString());
      throw new SchemaNotFoundException(response.getStatus(), "Schema is not found");
    }
    String schema = extractSchema(response);
    return schema;
  }

  /**
   *
   * @param json json
   * @param keyStore keystore location
   * @param keystorePwd keystore password
   */
  private static void jsonAddKeyStore(JSONObject json, String keyStore, String keystorePwd) {
    json.append(Constants.JSON_KEYSTOREPWD, keystorePwd);

    try {
      FileInputStream kfin = new FileInputStream(new File(keyStore));
      byte[] kStoreBlob = ByteStreams.toByteArray(kfin);
      String keystorString = Base64.encodeBase64String(kStoreBlob);
      json.append(Constants.JSON_KEYSTORE, keystorString);
    } catch (IOException ex) {
      LOGGER.log(Level.SEVERE, null, ex);
    }
  }

  /**
   *
   * @param uri uri
   * @param payload payload
   * @return Response
   */
  private static Response postJsonResponse(String uri, String payload) {
    ClientConfig config = new ClientConfig().register(LoggingFilter.class);
    Client client = ClientBuilder.newClient(config);
    WebTarget webTarget = client.target(uri);
    Invocation.Builder invocationBuilder = webTarget.request().accept(MediaType.APPLICATION_JSON);
    return invocationBuilder.post(Entity.entity(payload, MediaType.APPLICATION_JSON));
  }

  /**
   *
   * @param response
   * @return Avro schema as a string object in JSON format
   */
  private static String extractSchema(Response response) {
    String content = response.readEntity(String.class);
    //Extract fields from json
    JSONObject json = new JSONObject(content);
    String schema = json.getString(Constants.JSON_SCHEMA_CONTENTS);
    schema = tempHack(schema);
    return schema;
  }

  /**
   * Removes curly brackets from the schema return by Hopsworks.
   *
   * @param schema
   * @return Avro schema as a string object in JSON format
   */
  private static String tempHack(String schema) {
    int actualSchema = schema.indexOf('{');
    return schema.substring(actualSchema);
  }
}
