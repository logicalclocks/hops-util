package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.FeaturestoreRestClient;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.ops.FeaturestoreOp;

import javax.xml.bind.JAXBException;

/**
 * Builder class for Update-MetadataCache operation on the Hopsworks Featurestore
 */
public class FeaturestoreUpdateMetadataCache extends FeaturestoreOp {
  
  /**
   * Constructor
   */
  public FeaturestoreUpdateMetadataCache() {
    super("");
  }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }
  
  /**
   * Updates the metadata cache for a specified featurestore
   *
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   */
  public void write()
    throws JAXBException, FeaturestoreNotFound {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    FeaturestoreHelper.setFeaturestoreMetadataCache(FeaturestoreRestClient.getFeaturestoreMetadataRest(featurestore));
  }
  
  public FeaturestoreUpdateMetadataCache setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
}
