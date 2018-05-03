package io.hops.util;

import io.hops.util.exceptions.CredentialsNotFoundException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.Response;

import io.hops.util.exceptions.HTTPSClientInitializationException;
import io.hops.util.exceptions.WorkflowManagerException;
import org.json.JSONObject;

/**
 * Class providing methods for building Spark job workflows in HopsWorks.
 * Detailed documentation on building workflows is available in the online Hops documentation.
 *
 */
public class WorkflowManager {

  private static final Logger LOG = Logger.getLogger(WorkflowManager.class.getName());

  /**
   * Periodically polls HopsWorks for the current job's state and blocks until it  has transitioned from it.
   * Users need to provide the IDs of the jobs for which to wait on. Method will return when all jobs have
   * transitioned from the current state, for example from Running to Finished. Default state on which to wait is
   * "Running".
   *
   * @param jobs ID(s) of the job(s) to wait for. If multiple IDs are provided, method will return when the state of
   * all jobs has transitioned from the {@code waitOnJobState}.
   * @return true if the {@code waitOnJobState} is no longer the current job state, false if the timeout was exceeded.
   * @throws io.hops.util.exceptions.CredentialsNotFoundException CredentialsNotFoundException
   * @throws java.lang.InterruptedException InterruptedException
   * @throws WorkflowManagerException WorkflowManagerException
   */
  public static boolean waitForJobs(Integer... jobs)
      throws CredentialsNotFoundException, InterruptedException, WorkflowManagerException {
    return WorkflowManager.waitForJobs(Constants.WAIT_JOBS_TIMEOUT, Constants.WAIT_JOBS_TIMEOUT_TIMEUNIT, jobs);
  }

  /**
   * Periodically polls HopsWorks for the current job's state and blocks until it has transitioned from it.
   * Users need to provide the IDs of the jobs for which to wait on. Method will return when all jobs have
   * transitioned from the current state, for example from Running to Finished. Default state on which to wait is
   * "Running".
   *
   * @param timeout How long to wait until the method returns. Default is 7 days.
   * @param timeoutUnit Time unit of the timeout. Default is 7 days.
   * @param jobs ID(s) of the job(s) to wait for. If multiple IDs are provided, method will return when the state of
   * all jobs has transitioned from the {@code waitOnJobState}.
   * @return true if the {@code waitOnJobState} is no longer the current job state, false if the timeout was exceeded.
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws java.lang.InterruptedException InterruptedException
   * @throws WorkflowManagerException WorkflowManagerException
   */
  public static boolean waitForJobs(long timeout, TimeUnit timeoutUnit, Integer[] jobs) throws
      CredentialsNotFoundException, InterruptedException, WorkflowManagerException{
    return WorkflowManager.waitForJobs(Constants.WAIT_JOBS_TIMEOUT, Constants.WAIT_JOBS_TIMEOUT_TIMEUNIT,
        Constants.WAIT_JOBS_RUNNING_STATE, jobs);
  }

  /**
   * Periodically polls HopsWorks for the current job's state and blocks until it has transitioned from it.
   * Users need to provide the IDs of the jobs for which to wait on. Method will return when all jobs have
   * transitioned from the current state, for example from Running to Finished. Default state on which to wait is
   * "Running".
   *
   * @param timeout How long to wait for jobs on a given {@code waitOnJobState} until a timeout occurs.
   * @param timeoutUnit Time unit of the timeout. Default is 7 days.
   * @param jobs ID(s) of the job(s) to wait for. If multiple IDs are provided, method will return when the state of
   * all jobs has transitioned from the {@code waitOnJobState}.
   * @param waitOnJobState Set true for waiting while {@code jobs} are running, false for waiting while {@code jobs}
   * are not running.
   * @return true if the {@code waitOnJobState} is no longer the current job state, false if the timeout was exceeded.
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws java.lang.InterruptedException InterruptedException
   * @throws WorkflowManagerException WorkflowManagerException
   */
  public static boolean waitForJobs(long timeout, TimeUnit timeoutUnit, boolean waitOnJobState, Integer... jobs) throws
      CredentialsNotFoundException, InterruptedException, WorkflowManagerException {
    return WorkflowManager.waitForJobs(Constants.WAIT_JOBS_INTERVAL, TimeUnit.MILLISECONDS, timeout, timeoutUnit,
        waitOnJobState, jobs);
  }

  /**
   * Periodically polls HopsWorks for the current job's state and blocks until it has transitioned from it.
   * Users need to provide the IDs of the jobs for which to wait on. Method will return when all jobs have
   * transitioned from the current state, for example from Running to Finished. Default state on which to wait is
   * "Running".
   *
   * @param jobs ID(s) of the job(s) to wait for. If multiple IDs are provided, method will return when the state of
   * all jobs has transitioned from the {@code waitOnJobState}.
   * @param pollingIntervalUnit Time unit of the polling interval.
   * @param timeout How long to wait for jobs on a given {@code waitOnJobState} until a timeout occurs.
   * @param timeoutUnit Time unit of the timeout. Default is 7 days.
   * @param waitOnJobState Set true for waiting while {@code jobs} are running, false for waiting while {@code jobs}
   * are not running.
   * @param pollingInterval The interval in milliseconds to periodically poll HopsWorks for jobs' status.
   * @return true if the {@code currentJobState} has transitioned, false if the timeout was exceeded.
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws java.lang.InterruptedException java.lang.InterruptedException
   * @throws WorkflowManagerException WorkflowManagerException
   */
  public static boolean waitForJobs(long pollingInterval, TimeUnit pollingIntervalUnit, long timeout,
      TimeUnit timeoutUnit, boolean waitOnJobState, Integer... jobs) throws
      CredentialsNotFoundException, InterruptedException, WorkflowManagerException {
    
    JSONObject json = new JSONObject();
    json.put(Constants.JSON_JOBIDS, jobs);
    json.put(Constants.JSON_JOBSTATE, waitOnJobState);
    
    boolean flag = true;
    long startTime = System.nanoTime();
    long elapsed;
    timeout = timeoutUnit.toNanos(timeout);
    pollingInterval = pollingIntervalUnit.toMillis(pollingInterval);
    while (flag) {
      String response;
      try {
        response = Hops.clientWrapper(json, "jobs").readEntity(String.class);
      } catch (HTTPSClientInitializationException e) {
        throw new WorkflowManagerException(e.getMessage());
      }
      
      LOG.log(Level.INFO, "Retrieved running jobs:{0}", response);
      JSONObject jobsJSON = new JSONObject(response);
      //Wait on job(s) which are currently running
      if (waitOnJobState && jobsJSON.getJSONArray(Constants.JSON_JOBIDS).length() == 0) {
        flag = false;
      } //Wait on job(s) which are currently NOT running
      else if (!waitOnJobState && jobsJSON.getJSONArray(Constants.JSON_JOBIDS).length() > 0) {
        flag = false;
      }
      Thread.sleep(pollingInterval);
      elapsed = System.nanoTime() - startTime;
      if (elapsed > timeout) {
        return false;
      }
    }
    return true;
  }

  /**
   * Send an email from the HopsWorks platform.
   *
   * @param dest Email recipient.
   * @param subject Email subject.
   * @param message Email message body.
   * @return Response of HTTP REST API call to HopsWorks.
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws WorkflowManagerException WorkflowManagerException
   */
  public static Response sendEmail(String dest, String subject, String message) throws
      CredentialsNotFoundException, WorkflowManagerException {
    JSONObject json = new JSONObject();
    json.append("dest", dest);
    json.append("subject", subject);
    json.append("message", message);
    try {
      return Hops.clientWrapper(json, "mail");
    } catch (HTTPSClientInitializationException e) {
      throw new WorkflowManagerException(e.getMessage());
    }
  }

  /**
   * Start a HopsWorks job by providing its ID. If multiple IDs are provided, all jobs will be started in parallel.
   *
   * @param jobIds IDs of the jobs to start.
   * @return Response object of the HTTP REST call.
   * @throws CredentialsNotFoundException CredentialsNotFoundException
   * @throws WorkflowManagerException WorkflowManagerException
   */
  public static Response startJobs(Integer... jobIds) throws CredentialsNotFoundException, WorkflowManagerException {
    JSONObject json = new JSONObject();
    json.put(Constants.JSON_JOBIDS, jobIds);
    try {
      return Hops.clientWrapper(json, "mail");
    } catch (HTTPSClientInitializationException e) {
      throw new WorkflowManagerException(e.getMessage());
    }
  }

}
