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
 * <p>
 */
public class SparkInfo {

  private static final Logger LOG = Logger.getLogger(SparkInfo.class.getName());
  private final Configuration hdfsConf;
  private Path marker;

  protected SparkInfo(String jobName) {
    hdfsConf = new Configuration();
    //Write marker file to hdfs
    marker = new org.apache.hadoop.fs.Path(File.separator + "Projects" + File.separator + HopsUtil.getProjectName()
        + File.separator + "Resources" + File.separator + ".marker-" + HopsUtil.getJobType().toLowerCase() + "-"
        + HopsUtil.getJobName() + "-" + HopsUtil.getAppId());

    try {
      FileSystem hdfs = marker.getFileSystem(hdfsConf);
      hdfs.createNewFile(marker);
      //hdfs.setPermission(marker, new FsPermission("744"));
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Could not create marker file for job:" + HopsUtil.getJobName() + ", appId:" + HopsUtil.
          getAppId(), ex);

    }
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
      FileSystem hdfs = marker.getFileSystem(hdfsConf);
      return !hdfs.exists(marker);
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Could not check existence of marker file", ex);
    }
    return false;
  }

}
