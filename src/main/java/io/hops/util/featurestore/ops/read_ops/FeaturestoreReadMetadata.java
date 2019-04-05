package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.featurestore.dtos.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.ops.FeaturestoreOp;

import javax.xml.bind.JAXBException;

/**
 * Builder class for Read-metadata operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadMetadata extends FeaturestoreOp {
  
  /**
   * Constructor
   */
  public FeaturestoreReadMetadata() {
    super("");
  }
  
  /**
   * Gets featurestore metadata
   *
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @return a spark dataframe with the featuregroup
   */
  public FeaturestoreMetadataDTO read() throws FeaturestoreNotFound,
    JAXBException {
    if(FeaturestoreHelper.getFeaturestoreMetadataCache() == null){
      Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
    }
    return FeaturestoreHelper.getFeaturestoreMetadataCache();
  }
  
  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
  public FeaturestoreReadMetadata setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
}
