package io.hops.util;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Wrapper providing spark runtime services to user, for example JavaStreamingContext of the application.
 * 
 */
public class SparkInfo {

  private static final Logger LOG = Logger.getLogger(SparkInfo.class.getName());
  private final Configuration hdfsConf;
  private Path marker;

  protected SparkInfo(String jobName) {
    hdfsConf = new Configuration();
    //Write marker file to hdfs
    marker = new org.apache.hadoop.fs.Path("/" +Constants.PROJECT_ROOT_DIR + "/" + Hops.getProjectName()
        + "/" + Constants.PROJECT_STAGING_DIR + File.separator + ".marker-" + Hops.getJobType().toLowerCase() + "-"
        + Hops.getJobName() + "-" + Hops.getAppId());

    try {
      FileSystem hdfs = marker.getFileSystem(hdfsConf);
      hdfs.createNewFile(marker);
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Could not create marker file for job:" + Hops.getJobName() + ", appId:" + Hops.
          getAppId(), ex);

    }
  }

  /**
   * Checks if the marker file for this streaming app is present and returns true otherwise as that indicates a
   * requested shutdown.
   * In Hopsworks, the marker file is automatically removed by clicking the 'Stop' button in the Job service.
   *
   * @return true is shutdown has been requested.
   */
  protected boolean isShutdownRequested() {
    try {
      FileSystem hdfs = marker.getFileSystem(hdfsConf);
      return !hdfs.exists(marker);
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Could not check existence of marker file", ex);
    }
    return false;
  }

}
