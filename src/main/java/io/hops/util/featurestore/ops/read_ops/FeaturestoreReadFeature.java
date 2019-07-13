package io.hops.util.featurestore.ops.read_ops;

import com.google.common.base.Strings;
import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.bind.JAXBException;
import java.util.List;

/**
 * Builder class for Read-Feature operation on the Hopsworks Featurestore
 */
public class FeaturestoreReadFeature extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the feature to read
   */
  public FeaturestoreReadFeature(String name) {
    super(name);
  }
  
  /**
   * Gets a feature from a featurestore and a specific featuregroup.
   *
   * @return a spark dataframe with the feature
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @throws HiveNotEnabled HiveNotEnabled
   */
  public Dataset<Row> read() throws FeaturestoreNotFound, JAXBException, HiveNotEnabled {
    try {
      return doGetFeature(getSpark(), name, Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read(),
        featurestore);
    } catch (Exception e) {
      Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
      return doGetFeature(getSpark(), name, Hops.getFeaturestoreMetadata().setFeaturestore(featurestore).read()
        , featurestore);
    }
  }
  
  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
  /**
   * Gets a feature from a featurestore and infers the featuregroup where the feature is located
   *
   * @param sparkSession the spark session
   * @param feature      the feature to get
   * @param featurestoreMetadata metadata of the featurestore to query
   * @param featurestore the featurestore to query
   * @return A dataframe with the feature
   */
  private Dataset<Row> doGetFeature(
      SparkSession sparkSession, String feature,
      FeaturestoreMetadataDTO featurestoreMetadata, String featurestore) {
    sparkSession = FeaturestoreHelper.sparkGetOrDefault(sparkSession);
    List<FeaturegroupDTO> featuregroupsMetadata = featurestoreMetadata.getFeaturegroups();
    if(Strings.isNullOrEmpty(featuregroup)) {
      return FeaturestoreHelper.getFeature(sparkSession, feature, featurestore, featuregroupsMetadata);
    } else {
      return FeaturestoreHelper.getFeature(sparkSession, feature, featurestore, featuregroup, version);
    }
  }
  
  public FeaturestoreReadFeature setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
  
  public FeaturestoreReadFeature setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreReadFeature setVersion(int version) {
    this.version = version;
    return this;
  }
  
  public FeaturestoreReadFeature setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreReadFeature setFeaturegroup(String featuregroup) {
    this.featuregroup = featuregroup;
    return this;
  }
  
}
