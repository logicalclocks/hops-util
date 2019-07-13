/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hops.util;

import io.hops.util.exceptions.HTTPSClientInitializationException;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.WorkflowManagerException;
import org.json.JSONObject;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class providing methods for building Spark job workflows in HopsWorks.
 * Detailed documentation on building workflows is available in the online Hops documentation.
 *
 */
@Deprecated
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
   * @throws java.lang.InterruptedException InterruptedException
   * @throws WorkflowManagerException WorkflowManagerException
   */
  public static boolean waitForJobs(String... jobs) throws InterruptedException, WorkflowManagerException {
    return WorkflowManager.waitForJobs(Constants.WAIT_JOBS_INTERVAL,
      Constants.WAIT_JOBS_TIMEOUT_TIMEUNIT,
      Constants.WAIT_JOBS_TIMEOUT,
      Constants.WAIT_JOBS_TIMEOUT_TIMEUNIT,
      jobs);
  }
  
  
  /**
   * Wait until all jobs have been reached a final state.
   *
   * @param jobs names of the job(s) to wait on.
   * @param pollingIntervalUnit Time unit of the polling interval.
   * @param timeout How long to wait for jobs on a given {@code waitOnJobState} until a timeout occurs.
   * @param timeoutUnit Time unit of the timeout. Default is 7 days.
   * @param pollingInterval The interval in milliseconds to periodically poll HopsWorks for jobs' status.
   * @return true if the {@code currentJobState} has transitioned, false if the timeout was exceeded.
   * @throws java.lang.InterruptedException java.lang.InterruptedException
   * @throws WorkflowManagerException WorkflowManagerException
   */
  public static boolean waitForJobs(long pollingInterval, TimeUnit pollingIntervalUnit, long timeout,
    TimeUnit timeoutUnit, String... jobs) throws InterruptedException, WorkflowManagerException {
    
    boolean poll = true;
    long startTime = System.nanoTime();
    long elapsed;
    timeout = timeoutUnit.toNanos(timeout);
    pollingInterval = pollingIntervalUnit.toMillis(pollingInterval);
    while (poll) {
      //Loop through all jobs, if all have reach final state then set flag to false and exit
      for (String job : jobs) {
        String response;
        try {
          response =
            Hops.clientWrapper("/project/" + Hops.getProjectId() + "/jobs/" + job
                + "/executions?sort_by=submissiontime:desc&filter_by=state_neq:finished&filter_by=state_neq:failed" +
                "&filter_by=state_neq:killed&filter_by=state_neq:framework_failure" +
                "&filter_by=state_neq:app_master_start_failed&filter_by=state_neq:initialization_failed",
              HttpMethod.GET, null).readEntity(String.class);
        } catch (HTTPSClientInitializationException | JWTNotFoundException e) {
          throw new WorkflowManagerException(e.getMessage());
        }
        
        LOG.log(Level.INFO, "Retrieved running jobs:{0}", response);
        JSONObject jobsJSON = new JSONObject(response);
        //If job is still running set poll to false
        if (jobsJSON.getInt("count") == 0) {
          poll = false;
          break;
        }
      }
      LOG.info("Waiting...");
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
   * @throws WorkflowManagerException WorkflowManagerException
   */
  public static Response sendEmail(String dest, String subject, String message) throws WorkflowManagerException {
    JSONObject json = new JSONObject();
    json.append("dest", dest);
    json.append("subject", subject);
    json.append("message", message);
    try {
      return Hops.clientWrapper(json, "mail", HttpMethod.POST, null);
    } catch (HTTPSClientInitializationException | JWTNotFoundException e) {
      throw new WorkflowManagerException(e.getMessage());
    }
  }
  
  /**
   * Start one or multiple HopsWorks job(s) by providing their names. If multiple names are provided, all jobs will
   * be started in parallel.
   *
   * @param jobs names of jobs to start.
   * @throws WorkflowManagerException WorkflowManagerException
   */
  public static void startJobs(String... jobs) throws WorkflowManagerException {
    for (String job : jobs) {
      startJob(job);
    }
  }
  
  /**
   * Start a HopsWorks job by providing its name.
   *
   * @param job name of job to start.
   * @return Response object of the HTTP REST call.
   * @throws WorkflowManagerException WorkflowManagerException
   */
  public static Response startJob(String job) throws WorkflowManagerException {
    try {
      return Hops.clientWrapper("/project/" + Hops.getProjectId() + "/jobs/" + job + "/executions",
          HttpMethod.POST, null);
    } catch (HTTPSClientInitializationException | JWTNotFoundException e) {
      throw new WorkflowManagerException(e.getMessage());
    }
  }
}
