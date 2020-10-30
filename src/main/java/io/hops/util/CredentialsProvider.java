/*
 * This file is part of Hopsworks
 * Copyright (C) 2020, Logical Clocks AB. All rights reserved
 *
 * Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */
package io.hops.util;

import io.hops.util.cloud.Credentials;
import io.hops.util.exceptions.CloudCredentialException;
import io.hops.util.exceptions.HTTPSClientInitializationException;
import io.hops.util.exceptions.JWTNotFoundException;
import org.apache.spark.sql.SparkSession;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * AWS temporary credential provider.
 */
public class CredentialsProvider {
  private static final Logger LOG = Logger.getLogger(CredentialsProvider.class.getName());
  
  private CredentialsProvider() {
  }
  
  /**
   * Assume the default role if there exists a default role for the user in current project
   * @return
   * @throws CloudCredentialException
   */
  public static Credentials assumeRole() throws CloudCredentialException {
    return assumeRole(null, null, 0);
  }
  
  /**
   * Get temporary credentials and set spark context hadoop configuration
   * @param role
   * @return Credentials
   * @throws CloudCredentialException
   */
  public static Credentials assumeRole(String role) throws CloudCredentialException {
    return assumeRole(role, null, 0);
  }
  
  /**
   * Get temporary credentials and set spark context hadoop configuration and system properties.
   * @param role
   * @param roleSessionName
   * @param durationSeconds
   * @return Credentials
   * @throws CloudCredentialException
   */
  public static Credentials assumeRole(String role, String roleSessionName, int durationSeconds)
    throws CloudCredentialException {
    Response response;
    try {
      HashMap<String, Object> queryParams = new HashMap<>();
      if (role != null && !role.isEmpty()) {
        queryParams.put(Constants.HOPSWORKS_CLOUD_SESSION_TOKEN_RESOURCE_QUERY_ROLE, role);
      }
      if (roleSessionName != null && roleSessionName.isEmpty()) {
        queryParams.put(Constants.HOPSWORKS_CLOUD_SESSION_TOKEN_RESOURCE_QUERY_SESSION, roleSessionName);
      }
      if (durationSeconds > 0) {
        queryParams.put(Constants.HOPSWORKS_CLOUD_SESSION_TOKEN_RESOURCE_QUERY_SESSION_DURATION, durationSeconds);
      }
      response = Hops.clientWrapper("/" + Constants.HOPSWORKS_REST_PROJECT_RESOURCE + "/" + Hops.getProjectId() + "/"
          + Constants.HOPSWORKS_CLOUD_RESOURCE + "/" + Constants.HOPSWORKS_AWS_CLOUD_SESSION_TOKEN_RESOURCE,
          HttpMethod.GET, queryParams);
    } catch (HTTPSClientInitializationException | JWTNotFoundException e) {
      throw new CloudCredentialException(e.getMessage());
    }
    LOG.log(Level.FINE, "******* response.getStatusInfo():" + response.getStatusInfo());
    JSONObject jsonObject = getResponse(response);
    Credentials credentials = getCredentialsFromJson(jsonObject);
    setSparkHadoopConf(credentials);
    setSystemProperties(credentials);
    return credentials;
  }
  
  /**
   * Get all roles mapped to the current project
   * @return list of roles
   * @throws CloudCredentialException
   */
  public static String[] getRoles() throws CloudCredentialException {
    JSONObject roles = getCloudRoles(null);
    JSONArray items =  roles.getJSONArray(Constants.JSON_ARRAY_ITEMS);
    String[] cloudRoles = new String[items.length()];
    for (int i = 0; i < items.length(); i++) {
      JSONObject role = (JSONObject) items.get(i);
      cloudRoles[i] = (String) role.get(Constants.JSON_CLOUD_ROLE);
    }
    return cloudRoles;
  }
  
  /**
   * Get a role arn mapped to the current project by id
   * @param id
   * @return role
   * @throws CloudCredentialException
   */
  public static String getRole(Integer id) throws CloudCredentialException {
    return getCloudRoles(id.toString()).getString(Constants.JSON_CLOUD_ROLE);
  }
  
  /**
   * Get the default role arn mapped to the current project
   * @return
   * @throws CloudCredentialException
   */
  public static String getRole() throws CloudCredentialException {
    return getCloudRoles("default").getString(Constants.JSON_CLOUD_ROLE);
  }
  
  private static JSONObject getCloudRoles(String id) throws CloudCredentialException {
    Response response;
    String byId = id == null? "": "/" + id;
    try {
      response = Hops.clientWrapper("/" + Constants.HOPSWORKS_REST_PROJECT_RESOURCE + "/" + Hops.getProjectId() + "/" +
        Constants.HOPSWORKS_CLOUD_RESOURCE + "/" + Constants.HOPSWORKS_CLOUD_ROLE_MAPPINGS_RESOURCE + byId,
        HttpMethod.GET, null);
    } catch (HTTPSClientInitializationException | JWTNotFoundException e) {
      throw new CloudCredentialException(e.getMessage());
    }
    LOG.log(Level.FINE, "******* response.getStatusInfo():" + response.getStatusInfo());
    return getResponse(response);
  }
  
  private static void setSparkHadoopConf(Credentials credentials) {
    if (!System.getenv().containsKey(Constants.SPARK_IS_DRIVER_ENV)) {
      return;
    }
    SparkSession spark = Hops.findSpark();
    spark.sparkContext().hadoopConfiguration().set(Constants.S3_CREDENTIAL_PROVIDER_ENV,
      Constants.S3_TEMPORARY_CREDENTIAL_PROVIDER);
    spark.sparkContext().hadoopConfiguration().set(Constants.S3_ACCESS_KEY_ENV, credentials.getAccessKeyId());
    spark.sparkContext().hadoopConfiguration().set(Constants.S3_SECRET_KEY_ENV, credentials.getSecretAccessKey());
    spark.sparkContext().hadoopConfiguration().set(Constants.S3_SESSION_KEY_ENV, credentials.getSessionToken());
  }
  
  private static void setSystemProperties(Credentials credentials) {
    Hops.setEnv(Constants.AWS_ACCESS_KEY_ID_ENV, credentials.getAccessKeyId());
    Hops.setEnv(Constants.AWS_SECRET_ACCESS_KEY_ENV, credentials.getSecretAccessKey());
    Hops.setEnv(Constants.AWS_SESSION_TOKEN_ENV, credentials.getSessionToken());
  }
  
  private static Credentials getCredentialsFromJson(JSONObject jsonObject) {
    Credentials credentials = new Credentials();
    if (jsonObject.has(Constants.JSON_ACCESS_KEY_ID)) {
      credentials.setAccessKeyId(jsonObject.getString(Constants.JSON_ACCESS_KEY_ID));
    }
    if (jsonObject.has(Constants.JSON_SECRET_ACCESS_KEY_ID)) {
      credentials.setSecretAccessKey(jsonObject.getString(Constants.JSON_SECRET_ACCESS_KEY_ID));
    }
    if (jsonObject.has(Constants.JSON_SESSION_TOKEN_ID)) {
      credentials.setSessionToken(jsonObject.getString(Constants.JSON_SESSION_TOKEN_ID));
    }
    return credentials;
  }
  
  private static JSONObject getResponse(Response response) throws CloudCredentialException {
    Response.Status.Family statusFamily = response.getStatusInfo().getFamily();
    if (response.getMediaType() != null &&
      MediaType.APPLICATION_JSON_TYPE.getSubtype().equals(response.getMediaType().getSubtype())) {
      try {
        final String responseEntity = response.readEntity(String.class);
        JSONObject content = new JSONObject(responseEntity);
        if (statusFamily == Response.Status.Family.INFORMATIONAL || statusFamily == Response.Status.Family.SUCCESSFUL) {
          return content;
        } else {
          String errorMsg =
            content.getString(Constants.JSON_ERROR_MSG) + " " + content.getString(Constants.JSON_USR_MSG);
          throw new CloudCredentialException(errorMsg);
        }
      } catch (ProcessingException e) {
        throw new CloudCredentialException(e.getMessage() + " Status: " + response.getStatus());
      }
    } else {
      throw new CloudCredentialException("Cannot Connect To Server. Got status: " + response.getStatus());
    }
  }
  
  public static class AssumeRoleRequest {
    private String roleArn;
    private String roleSessionName;
    private int durationSeconds;
  
    private AssumeRoleRequest() {
    }
    
    public static AssumeRoleRequest builder() {
      return new AssumeRoleRequest();
    }
    
    public Credentials send() throws CloudCredentialException {
      return CredentialsProvider.assumeRole(roleArn, roleSessionName, durationSeconds);
    }
  
    public AssumeRoleRequest setRoleArn(String roleArn) {
      this.roleArn = roleArn;
      return this;
    }
  
    public AssumeRoleRequest setRoleSessionName(String roleSessionName) {
      this.roleSessionName = roleSessionName;
      return this;
    }
  
    public AssumeRoleRequest setDurationSeconds(int durationSeconds) {
      this.durationSeconds = durationSeconds;
      return this;
    }
  }
  
}
