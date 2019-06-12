package io.hops.util.featurestore.ops.read_ops;

import io.hops.util.exceptions.HiveNotEnabled;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.ops.FeaturestoreOp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Builder class for SQL query on feature store
 */
public class FeaturestoreSQLQuery extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name the sql query
   */
  public FeaturestoreSQLQuery(String name) {
    super(name);
  }
  
  /**
   * Runs an SQL query against the Featurestore Hive Database
   *
   * @return a spark dataframe with the results
   * @throws HiveNotEnabled HiveNotEnabled
   */
  public Dataset<Row> read() throws HiveNotEnabled {
    return FeaturestoreHelper.queryFeaturestore(getSpark(), name, featurestore);
  }
  
  /**
   * Method call to execute write operation
   */
  public void write(){
    throw new UnsupportedOperationException("write() is not supported on a read operation");
  }
  
  public FeaturestoreSQLQuery setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreSQLQuery setSpark(SparkSession spark) {
    this.spark = spark;
    return this;
  }
}
