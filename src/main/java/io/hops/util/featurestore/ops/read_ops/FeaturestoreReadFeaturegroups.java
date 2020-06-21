package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.featuregroup.CachedFeaturegroupDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.dtos.featuregroup.OnDemandFeaturegroupDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;

import javax.xml.bind.JAXBException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builder class for Read-Featuregroups operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadFeaturegroups extends FeaturestoreOp {
  
  /**
   * Constructor
   */
  public FeaturestoreReadFeaturegroups() {
    super("");
  }
  
  /**
   * Gets a list of all featuregroups from a particular featurestore
   *
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @return a list of the featuregroups
   */
  public List<String> read() throws FeaturestoreNotFound, JAXBException {
    return Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read()
      .getFeaturegroups().stream()
      .filter(fg -> filterOnline(fg))
      .map(fg -> FeaturestoreHelper.getTableName(fg.getName(), fg.getVersion())).collect(Collectors.toList());
  }
  
  /**
   * @param featuregroupDTO the featuregroup to filter
   * @return true if online filter is on and the featuregroup is online, otherwise false
   */
  private Boolean filterOnline(FeaturegroupDTO featuregroupDTO){
    if(online == null || !online) {
      return true;
    }
    if(featuregroupDTO instanceof OnDemandFeaturegroupDTO) {
      return false;
    } else {
      CachedFeaturegroupDTO cachedFeaturegroupDTO = (CachedFeaturegroupDTO) featuregroupDTO;
      return cachedFeaturegroupDTO.getOnlineEnabled();
    }
  }
  
  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
  public FeaturestoreReadFeaturegroups setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreReadFeaturegroups setOnline(Boolean online) {
    this.online = online;
    return this;
  }
  
}
