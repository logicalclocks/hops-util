package io.hops.util.dela;

import com.google.common.io.ByteStreams;

import io.hops.util.HopsUtil;
import io.hops.util.SchemaNotFoundException;
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

public class DelaHelper {

  private static final Logger logger = Logger.getLogger(DelaHelper.class.getName());

  public static DelaConsumer getHopsConsumer(int projectId, String topicName, String brokerEndpoint, String restEndpoint,
      String keyStore, String trustStore, String keystorePwd, String truststorePwd)
      throws SchemaNotFoundException {
    logger.log(Level.INFO, "projectId:{0}, topicName:{1}, brokerE:{2}, restE:{3}, keyS:{4}, trustS:{5}, keysP:{6}, "
        + "trustsP:{7}", new Object[]{projectId, topicName, brokerEndpoint, restEndpoint, keyStore, trustStore,
          keystorePwd, truststorePwd});
    HopsUtil.setup(projectId, topicName, brokerEndpoint, restEndpoint, keyStore, trustStore, keystorePwd, truststorePwd);
    String stringSchema = getSchemaByTopic(HopsUtil.getRestEndpoint(), projectId, topicName);
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(stringSchema);
    return new DelaConsumer(topicName, schema);
  }

  public static DelaProducer getHopsProducer(int projectId, String topicName, String brokerEndpoint, String restEndpoint,
      String keyStore, String trustStore, String keystorePwd, String truststorePwd, long lingerDelay)
      throws SchemaNotFoundException {
    logger.log(Level.INFO, "projectId:{0}, topicName:{1}, brokerE:{2}, restE:{3}, keyS:{4}, trustS:{5}, keysP:{6}, "
        + "trustsP:{7}, linger:{8}", new Object[]{projectId, topicName, brokerEndpoint, restEndpoint, keyStore,
          trustStore,
          keystorePwd, truststorePwd, lingerDelay});
    HopsUtil.setup(projectId, topicName, brokerEndpoint, restEndpoint, keyStore, trustStore, keystorePwd, truststorePwd);
    String stringSchema = getSchemaByTopic(HopsUtil.getRestEndpoint(), projectId, topicName);
    logger.log(Level.INFO, "schema:{0}", new Object[]{stringSchema});
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(stringSchema);
    return new DelaProducer(topicName, schema, lingerDelay);
  }

  public static String getSchemaByTopic(String restEndpoint, int projectId, String topicName)
      throws SchemaNotFoundException {
    return getSchemaByTopic(restEndpoint, projectId, topicName, Integer.MIN_VALUE);
  }

  public static String getSchemaByTopic(String restEndpoint, int projectId, String topicName, int versionId)
      throws SchemaNotFoundException {

    JSONObject json = new JSONObject();
    jsonAddKeyStore(json, HopsUtil.getKeyStore(), HopsUtil.getKeystorePwd());
    json.append("topicName", topicName);
    if (versionId > 0) {
      json.append("version", versionId);
    }

    String uri = HopsUtil.getRestEndpoint() + "/schema";
    logger.info("getting schema:" + uri);
    Response response = postJsonResponse(uri, json.toString());
    if (response == null) {
      throw new SchemaNotFoundException("Could not reach schema endpoint");
    } else if (response.getStatus() != 200) {
      logger.warning("error code:" + response.getStatus());
      logger.warning("response:" + response.toString());
      throw new SchemaNotFoundException(response.getStatus(), "Schema is not found");
    }
    String schema = extractSchema(response);
    return schema;
  }

  private static void jsonAddKeyStore(JSONObject json, String keyStore, String keystorePwd) {
    json.append("keyStorePwd", keystorePwd);

    try {
      FileInputStream kfin = new FileInputStream(new File(keyStore));
      byte[] kStoreBlob = ByteStreams.toByteArray(kfin);
      String keystorString = Base64.encodeBase64String(kStoreBlob);
      json.append("keyStore", keystorString);
    } catch (IOException ex) {
      logger.log(Level.SEVERE, null, ex);
    }
  }

  private static Response getResponse(String uri) {
    ClientConfig config = new ClientConfig().register(LoggingFilter.class);
    Client client = ClientBuilder.newClient(config);
    WebTarget webTarget = client.target(uri);
    Invocation.Builder invocationBuilder = webTarget.request().accept(MediaType.APPLICATION_JSON);
    return invocationBuilder.get();
  }

  private static Response postJsonResponse(String uri, String payload) {
    ClientConfig config = new ClientConfig().register(LoggingFilter.class);
    Client client = ClientBuilder.newClient(config);
    WebTarget webTarget = client.target(uri);
    Invocation.Builder invocationBuilder = webTarget.request().accept(MediaType.APPLICATION_JSON);
    return invocationBuilder.post(Entity.entity(payload, MediaType.APPLICATION_JSON));
  }

  private static String extractSchema(Response response) {
    String content = response.readEntity(String.class);
    //Extract fields from json
    JSONObject json = new JSONObject(content);
    String schema = json.getString("contents");
    schema = tempHack(schema);
    return schema;
  }

  private static String tempHack(String schema) {
    int actualSchema = schema.indexOf('{');
    return schema.substring(actualSchema);
  }
}
