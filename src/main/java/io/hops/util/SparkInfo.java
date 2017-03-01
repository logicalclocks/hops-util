package io.hops.util;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Wrapper providing spark runtime services to user, for example JavaStreamingContext of the application.
 * <p>
 */
public class SparkInfo {

  private static final Logger LOG = Logger.getLogger(SparkInfo.class.getName());

  private JavaStreamingContext jssc;
  protected final SparkConf sparkConf;
  private final Configuration hdfsConf;
  private Path MARKER;
  private String appId;
  private boolean initialized;

  protected SparkInfo() {
    hdfsConf = new Configuration();
    sparkConf = new SparkConf().setAppName(HopsUtil.getJobName());
  }

  /**
   * Creates and returns a Spark JavaStreamingContext. It also generates a marker file for gracefully stopping
   * the streaming job.
   * Creates and sets the SparkConf as well.
   *
   * @param duration Duration in seconds
   * @return
   */
  protected JavaStreamingContext getJavaStreamingContext(long duration) {
    if (!initialized) {

      jssc = new JavaStreamingContext(sparkConf, Durations.seconds(duration));
      appId = jssc.sparkContext().getConf().getAppId();

      //Write marker file to hdfs
      MARKER = new org.apache.hadoop.fs.Path(File.separator + "Projects" + File.separator + HopsUtil.getProjectName()
          + File.separator + "Resources" + File.separator + ".marker-" + HopsUtil.getJobType().toLowerCase() + "-"
          + HopsUtil.getJobName() + "-" + appId);

      try {
        FileSystem hdfs = MARKER.getFileSystem(hdfsConf);
        hdfs.createNewFile(MARKER);
      } catch (IOException ex) {
        LOG.log(Level.SEVERE, "Could not create marker file for job:" + HopsUtil.getJobName() + ", appId:" + getAppId(
            jssc), ex);

      }
      initialized = true;
    }
    return jssc;
  }

  protected JavaStreamingContext getJavaStreamingContext() {
    return jssc;
  }

  protected JavaStreamingContext createJavaStreamingContext(long duration) {
    return new JavaStreamingContext(sparkConf, Durations.seconds(duration));
  }

  protected String getAppId(JavaStreamingContext jssc) {
    return jssc.sparkContext().getConf().getAppId();
  }

  /**
   * Checks if the marker file for this streaming app is present and returns true otherwise as that indicates a
   * requested shutdown.
   * In Hopsworks, the marker file is automatically removed by clicking the 'Stop' button in the Job service.
   *
   * @return
   */
  protected boolean isShutdownRequested() {
    try {
      FileSystem hdfs = MARKER.getFileSystem(hdfsConf);
      return !hdfs.exists(MARKER);
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Could not check existence of marker file", ex);
    }
    return false;
  }

  /**
   * Returns a new SparkSession.
   *
   * @return
   */
  protected SparkSession getSparkSession() {
    return SparkSession.builder().config(sparkConf).getOrCreate();
  }

}
