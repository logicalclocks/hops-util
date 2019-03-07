package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.Constants;
import io.hops.util.FeaturestoreRestClient;
import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.FeaturestoresNotFound;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;
import java.util.ArrayList;
import java.util.List;

/**
 * Builder class for Read-List-of-Featurestores operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadProjectFeaturestores extends FeaturestoreOp {
  
  /**
   * Constructor
   */
  public FeaturestoreReadProjectFeaturestores() {
    super("");
  }
  
  /**
   * Gets a list of featurestores accessible in the project (i.e the project's own featurestore
   * and the featurestores shared with the project)
   *
   * @return a list of names of the featurestores accessible by this project
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @throws FeaturestoresNotFound FeaturestoresNotFound
   * @throws JWTNotFoundException JWTNotFoundException
   */
  public List<String> read() throws FeaturestoreNotFound,
    JAXBException, FeaturestoresNotFound, JWTNotFoundException {
    if(FeaturestoreHelper.getFeaturestoreMetadataCache() == null){
      Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
    }
    Response response = FeaturestoreRestClient.getFeaturestoresForProjectRest();
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
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
}
