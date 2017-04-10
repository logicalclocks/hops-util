package io.hops.util.dela;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import io.hops.util.SchemaNotFoundException;
import java.util.logging.Logger;
import javax.ws.rs.core.MediaType;
import org.apache.avro.Schema;
import org.json.JSONObject;

public class DelaHelper {

  private static final Logger logger = Logger.getLogger(DelaHelper.class.getName());

  public static DelaConsumer getHopsConsumer(String restEndpoint, int projectId, String topic) 
    throws SchemaNotFoundException {
    String stringSchema = getSchemaByTopic(restEndpoint, projectId, topic);
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(stringSchema);
    return new DelaConsumer(topic, schema);
  }

  public static DelaProducer getHopsProducer(String restEndpoint, int projectId, String topic, long lingerDelay)
    throws SchemaNotFoundException {
    String stringSchema = getSchemaByTopic(restEndpoint, projectId, topic);
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(stringSchema);
    return new DelaProducer(topic, schema, lingerDelay);
  }

  public static String getSchemaByTopic(String restEndpoint, int projectId, String topicName) 
    throws SchemaNotFoundException {
    return getSchemaByTopic(restEndpoint, projectId, topicName, Integer.MIN_VALUE);
  }

  public static String getSchemaByTopic(String restEndpoint, int projectId, String topicName, int versionId) 
    throws SchemaNotFoundException {

    String uri = restEndpoint + "/" + projectId + "/kafka/schema/" + topicName;
    if (versionId > 0) {
      uri += "/" + versionId;
    }
    logger.info("getting schema:" + uri);
    ClientResponse response = getResponse(uri);
    if (response == null) {
      throw new SchemaNotFoundException("Could not reach schema endpoint");
    } else if (response.getStatus() != 200) {
      throw new SchemaNotFoundException(response.getStatus(), "Schema is not found");
    }
    String schema = extractSchema(response);
    return schema;

  }

  public static String getSchemaByName(String restEndpoint, String jSessionId, int projectId, String schemaName,
    int schemaVersion) throws SchemaNotFoundException {
    String uri = restEndpoint + "/" + projectId + "/kafka/showSchema/" + schemaName + "/" + schemaVersion;
    logger.info("getting schema:" + uri);
    ClientResponse response = getResponse(uri);
    if (response == null) {
      throw new SchemaNotFoundException("Could not reach schema endpoint");
    } else if (response.getStatus() != 200) {
      throw new SchemaNotFoundException(response.getStatus(), "Schema is not found");
    }
    String schema = extractSchema(response);
    return schema;
  }

  private static ClientResponse getResponse(String uri) {
    ClientConfig config = new DefaultClientConfig();
    Client client = Client.create(config);
    WebResource service = client.resource(uri);
    return service.type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
  }
  
  private static ClientResponse postResponse(String uri, String payload) {
    ClientConfig config = new DefaultClientConfig();
    Client client = Client.create(config);
    WebResource service = client.resource(uri);
    return service.type(MediaType.APPLICATION_JSON).post(ClientResponse.class, payload);
  }

  private static String extractSchema(ClientResponse response) {
    String content = response.getEntity(String.class);
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
