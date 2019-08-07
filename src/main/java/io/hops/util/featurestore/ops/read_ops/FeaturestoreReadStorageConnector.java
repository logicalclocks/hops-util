package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.StorageConnectorNotFound;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreStorageConnectorDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;

import javax.xml.bind.JAXBException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builder class for Read-Storage Connector operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadStorageConnector extends FeaturestoreOp {

  /**
   * Constructor
   */
  public FeaturestoreReadStorageConnector() {
    super("");
  }
  
  /**
   * Gets a storage connector with a particular name
   *
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @throws StorageConnectorNotFound StorageConnectorNotFound
   * @return storage connector with the given name
   */
  public FeaturestoreStorageConnectorDTO read() throws FeaturestoreNotFound, JAXBException, StorageConnectorNotFound {
    List<FeaturestoreStorageConnectorDTO> filteredStorageConnectors =
        Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read().getStorageConnectors().stream()
      .filter(sc -> sc.getName().equalsIgnoreCase(name)).collect(Collectors.toList());
    if(!filteredStorageConnectors.isEmpty()){
      return filteredStorageConnectors.get(0);
    } else {
      throw new StorageConnectorNotFound("Storage connector with name: " + name + ", was not found");
    }
  }
  
  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
  public FeaturestoreReadStorageConnector setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }

  public FeaturestoreReadStorageConnector setName(String name) {
    this.name = name;
    return this;
  }
  
}
