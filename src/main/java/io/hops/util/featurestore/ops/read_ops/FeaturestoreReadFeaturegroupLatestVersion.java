package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.featurestore.dtos.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.FeaturegroupDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;

import javax.xml.bind.JAXBException;
import java.util.List;

/**
 * Builder class for Read-LatestFeaturegroupVersion operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadFeaturegroupLatestVersion extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the featuregroup
   */
  public FeaturestoreReadFeaturegroupLatestVersion(String name) {
    super(name);
  }
  
  /**
   * Gets the latest version of a featuregroup in the featurestore
   *
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @return the latest version of the featuregroup
   */
  public Integer read() throws FeaturestoreNotFound, JAXBException {
    try {
      return doGetLatestFeaturegroupVersion(name, Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read());
    } catch (Exception e) {
      Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
      return doGetLatestFeaturegroupVersion(name, Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read());
    }
  }
  
  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
  public FeaturestoreReadFeaturegroupLatestVersion setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  
  /**
   * Extrects the latest version of a featuregroup from the metadata
   *
   * @param featuregroupName name of the featuregroup
   * @param featurestoreMetadata metadata of the featurestore
   * @return the latest version of the feature group
   */
  private static int doGetLatestFeaturegroupVersion(
    String featuregroupName, FeaturestoreMetadataDTO featurestoreMetadata) {
    List<FeaturegroupDTO> featuregroupDTOList = featurestoreMetadata.getFeaturegroups();
    return FeaturestoreHelper.getLatestFeaturegroupVersion(featuregroupDTOList, featuregroupName);
  }
  
}
