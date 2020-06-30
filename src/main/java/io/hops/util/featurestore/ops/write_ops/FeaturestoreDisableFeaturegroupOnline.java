package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.FeaturestoreRestClient;
import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturegroupDisableOnlineError;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.dtos.featuregroup.OnDemandFeaturegroupDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;

import javax.xml.bind.JAXBException;

/**
 * Builder class for Disable Online Serving for a Featuregroup operation on the Hopsworks Featurestore.
 */
public class FeaturestoreDisableFeaturegroupOnline extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the featuregroup to disable online serving for
   */
  public FeaturestoreDisableFeaturegroupOnline(String name) {
    super(name);
  }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }
  
  /**
   * Disables online feature serving for a featuregroup
   *
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   * @throws FeaturegroupDisableOnlineError FeaturegroupDisableOnlineError
   */
  public void write()
    throws JAXBException, FeaturestoreNotFound, JWTNotFoundException, FeaturegroupDoesNotExistError,
    FeaturegroupDisableOnlineError {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    FeaturestoreMetadataDTO featurestoreMetadata = FeaturestoreHelper.getFeaturestoreMetadataCache();
    FeaturegroupDTO featuregroupDTO = FeaturestoreHelper.findFeaturegroup(featurestoreMetadata.getFeaturegroups(),
      name, version);
    if(featuregroupDTO instanceof OnDemandFeaturegroupDTO) {
      throw new IllegalArgumentException("Cannot Disable Online Feature Serving on an on-demand " +
        "feature group, this operation is only supported for cached feature groups");
    }
    FeaturestoreRestClient.disableFeaturegroupOnlineRest(featuregroupDTO,
      FeaturestoreHelper.getFeaturegroupDtoTypeStr(featurestoreMetadata.getSettings(), false));
    //Update metadata cache since we modified feature group
    Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
  }
  
  public FeaturestoreDisableFeaturegroupOnline setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreDisableFeaturegroupOnline setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreDisableFeaturegroupOnline setVersion(int version) {
    this.version = version;
    return this;
  }
  
}
