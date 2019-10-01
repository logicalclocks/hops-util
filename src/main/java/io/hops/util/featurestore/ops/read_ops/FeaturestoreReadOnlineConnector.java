package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreJdbcConnectorDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;

import javax.xml.bind.JAXBException;

/**
 * Builder class for Read-Feature operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadOnlineConnector extends FeaturestoreOp {
  
  /**
   * Constructor
   */
  public FeaturestoreReadOnlineConnector() {
    super("");
  }
  
  /**
   * Gets a JDBC Connector feature for online featurestore.
   *
   * @return DTO with information about the JDBC connector
   * @throws JAXBException
   * @throws FeaturestoreNotFound
   */
  public FeaturestoreJdbcConnectorDTO read() throws JAXBException, FeaturestoreNotFound {
    return FeaturestoreHelper.doGetOnlineFeaturestoreJdbcConnector(featurestore,
      new FeaturestoreReadMetadata().setFeaturestore(featurestore).read());
  }
  
  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
  
  public FeaturestoreReadOnlineConnector setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
}
