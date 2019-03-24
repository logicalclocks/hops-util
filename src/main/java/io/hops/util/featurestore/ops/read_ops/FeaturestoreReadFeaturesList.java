package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.featurestore.ops.FeaturestoreOp;

import javax.xml.bind.JAXBException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builder class for Read-FeaturesList operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadFeaturesList extends FeaturestoreOp {
  
  /**
   * Constructor
   */
  public FeaturestoreReadFeaturesList() {
    super("");
  }
  
  /**
   * Gets a list of all features from a particular featurestore
   *
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @return a spark dataframe with the featuregroup
   */
  public List<String> read() throws FeaturestoreNotFound, JAXBException {
    List<List<String>> featureNamesLists = Hops.getFeaturestoreMetadata().setFeaturestore(featurestore)
      .read().getFeaturegroups().stream()
      .map(fg -> fg.getFeatures().stream().map(f -> f.getName())
        .collect(Collectors.toList())).collect(Collectors.toList());
    List<String> featureNames = new ArrayList<>();
    featureNamesLists.stream().forEach(flist -> featureNames.addAll(flist));
    return featureNames;
  }
  
  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
  public FeaturestoreReadFeaturesList setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
}
