package io.hops.util;

import io.hops.util.exceptions.FeaturegroupCreationError;
import io.hops.util.exceptions.FeaturegroupDeletionError;
import io.hops.util.exceptions.FeaturegroupDisableOnlineError;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.FeaturegroupEnableOnlineError;
import io.hops.util.exceptions.FeaturegroupUpdateStatsError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.FeaturestoresNotFound;
import io.hops.util.exceptions.HTTPSClientInitializationException;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.TagError;
import io.hops.util.exceptions.TrainingDatasetCreationError;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreJdbcConnectorDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreStorageConnectorType;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetDTO;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Exposes featurestore RPC/Rest API
 */
public class FeaturestoreRestClient {
  
  private FeaturestoreRestClient(){}
  
  private static final Logger LOG = Logger.getLogger(FeaturestoreRestClient.class.getName());
  
  /**
   * Makes a REST call to Hopsworks to get metadata about a featurestore, this metadata is then used by
   * hops-util to infer how to JOIN featuregroups together etc.
   *
   * @param featurestore the featurestore to query metadata about
   * @return a list of featuregroups metadata
   * @throws FeaturestoreNotFound FeaturestoresNotFound
   * @throws JAXBException JAXBException
   */
  public static FeaturestoreMetadataDTO getFeaturestoreMetadataRest(String featurestore)
    throws FeaturestoreNotFound, JAXBException {
    LOG.log(Level.FINE, "Getting metadata for featurestore " + featurestore);
    
    Response response;
    try {
      response =
        Hops.clientWrapper(
          "/project/" + Hops.getProjectId() + "/" + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE + "/" +
            featurestore + "/" + Constants.HOPSWORKS_REST_FEATURESTORE_METADATA_RESOURCE,
          HttpMethod.GET, null);
    } catch (HTTPSClientInitializationException | JWTNotFoundException e) {
      throw new FeaturestoreNotFound(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      throw new FeaturestoreNotFound("Could not fetch metadata for featurestore:" + featurestore);
    }
    final String responseEntity = response.readEntity(String.class);
    
    JSONObject featurestoreMetadata = new JSONObject(responseEntity);
    return FeaturestoreHelper.parseFeaturestoreMetadataJson(featurestoreMetadata);
  }
  
  /**
   * Makes a REST call to Hopsworks for deleting
   * the contents of the featuregroup but keeps the featuregroup metadata
   *
   * @param featuregroupDTO DTO information about the feature group
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws FeaturegroupDeletionError FeaturegroupDeletionError
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   */
  public static void deleteTableContentsRest(
    FeaturegroupDTO featuregroupDTO)
    throws JWTNotFoundException, FeaturegroupDeletionError, JAXBException, FeaturestoreNotFound,
    FeaturegroupDoesNotExistError {
    LOG.log(Level.FINE, "Deleting table contents of featuregroup " + featuregroupDTO.getName() +
      "version: " + featuregroupDTO.getVersion() + " in featurestore: " + featuregroupDTO.getFeaturestoreName());
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(featuregroupDTO.getFeaturestoreName());
      int featuregroupId = FeaturestoreHelper.getFeaturegroupId(featuregroupDTO.getFeaturestoreName(),
        featuregroupDTO.getName(), featuregroupDTO.getVersion());
      Response response = Hops.clientWrapper(
        "/project/" + Hops.getProjectId() + "/" + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE + "/" +
          featurestoreId + "/" + Constants.HOPSWORKS_REST_FEATUREGROUPS_RESOURCE + "/" + featuregroupId + "/" +
          Constants.HOPSWORKS_REST_FEATUREGROUP_CLEAR_RESOURCE, HttpMethod.POST, null);
      LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
      if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
        throw new FeaturegroupDeletionError("Could not clear the contents of featuregroup:" +
          featuregroupDTO.getName() +
          " , response code: " + response.getStatusInfo().getStatusCode());
      }
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupDeletionError(e.getMessage());
    }
  }
  
  /**
   * Makes a REST call to Hopsworks for creating a new featuregroup from a spark dataframe.
   *
   * @param featuregroupDTO        the featurestore where the group will be created
   * @param featuregroupDTOType    the DTO type
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupCreationError FeaturegroupCreationError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   */
  public static void createFeaturegroupRest(FeaturegroupDTO featuregroupDTO, String featuregroupDTOType)
    throws JWTNotFoundException, JAXBException, FeaturegroupCreationError, FeaturestoreNotFound {
    LOG.log(Level.FINE, "Creating featuregroup " + featuregroupDTO.getName() +
      " in featurestore: " + featuregroupDTO.getFeaturestoreName());
    JSONObject json = FeaturestoreHelper.convertFeaturegroupDTOToJsonObject(featuregroupDTO);
    json.put(Constants.JSON_FEATURESTORE_ENTITY_TYPE, featuregroupDTOType);
    Response response;
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(featuregroupDTO.getFeaturestoreName());
      response = Hops.clientWrapper(json,
        "/project/" + Hops.getProjectId() + "/" + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE + "/" +
          featurestoreId + "/" + Constants.HOPSWORKS_REST_FEATUREGROUPS_RESOURCE,
        HttpMethod.POST, null);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupCreationError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.CREATED.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = Hops.parseHopsworksErrorResponse(response);
      throw new FeaturegroupCreationError("Could not create featuregroup:" + featuregroupDTO.getName() +
        " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
        + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
  }
  
  /**
   * Makes a REST call to Hopsworks for creating a new training dataset from a spark dataframe
   *
   * @param trainingDatasetDTO            DTO of the training dataset
   * @return the JSON response
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws JAXBException JAXBException
   * @throws TrainingDatasetCreationError TrainingDatasetCreationError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   */
  public static Response createTrainingDatasetRest(TrainingDatasetDTO trainingDatasetDTO)
    throws JWTNotFoundException, JAXBException, TrainingDatasetCreationError, FeaturestoreNotFound {
    LOG.log(Level.FINE, "Creating Training Dataset " + trainingDatasetDTO.getName() +
      " in featurestore: " + trainingDatasetDTO.getFeaturestoreName());
    JSONObject json = FeaturestoreHelper.convertTrainingDatasetDTOToJsonObject(trainingDatasetDTO);
    Response response;
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(trainingDatasetDTO.getFeaturestoreName());
      response = Hops.clientWrapper(json,
        "/project/" + Hops.getProjectId() + "/" + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE + "/" +
          featurestoreId + "/" + Constants.HOPSWORKS_REST_TRAININGDATASETS_RESOURCE,
        HttpMethod.POST, null);
    } catch (HTTPSClientInitializationException e) {
      throw new TrainingDatasetCreationError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.CREATED.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = Hops.parseHopsworksErrorResponse(response);
      throw new TrainingDatasetCreationError("Could not create trainingDataset:" + trainingDatasetDTO.getName() +
        " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
        + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
    return response;
  }
  
  /**
   * Makes a REST call to Hopsworks for getting the list of featurestores in the project
   *
   * @return the HTTP response
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws FeaturestoresNotFound FeaturestoresNotFound
   */
  public static Response getFeaturestoresForProjectRest()
    throws JWTNotFoundException, FeaturestoresNotFound {
    LOG.log(Level.FINE, "Getting featurestores for current project");
    Response response;
    try {
      response =
        Hops.clientWrapper("/project/" + Hops.getProjectId() + "/" +
            Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE, HttpMethod.GET, null);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturestoresNotFound(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      throw new FeaturestoresNotFound("Could not fetch featurestores for the current project");
    }
    return response;
  }
  
  /**
   * @param trainingDatasetDTO        DTO of the training dataset
   * @return the JSON response
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   */
  public static Response updateTrainingDatasetStatsRest(TrainingDatasetDTO trainingDatasetDTO)
      throws JWTNotFoundException, JAXBException, FeaturegroupUpdateStatsError,
      TrainingDatasetDoesNotExistError, FeaturestoreNotFound {

    LOG.log(Level.FINE, "Updating training dataset stats for: " + trainingDatasetDTO.getName() +
      " in featurestore: " + trainingDatasetDTO.getFeaturestoreName());
    JSONObject json = FeaturestoreHelper.convertTrainingDatasetDTOToJsonObject(trainingDatasetDTO);
    Response response = null;
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(trainingDatasetDTO.getFeaturestoreName());
      int trainingDatasetId = FeaturestoreHelper.getTrainingDatasetId(trainingDatasetDTO.getFeaturestoreName(),
        trainingDatasetDTO.getName(), trainingDatasetDTO.getVersion());
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put(Constants.JSON_FEATURESTORE_UPDATE_STATS_QUERY_PARAM, true);
      queryParams.put(Constants.JSON_FEATURESTORE_UPDATE_JOB_QUERY_PARAM, !trainingDatasetDTO.getJobs().isEmpty());
      response = Hops.clientWrapper(json,
        "/project/" + Hops.getProjectId() + "/" + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE + "/" +
          featurestoreId + "/" + Constants.HOPSWORKS_REST_TRAININGDATASETS_RESOURCE + "/" + trainingDatasetId,
        HttpMethod.PUT, queryParams);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupUpdateStatsError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = Hops.parseHopsworksErrorResponse(response);
      throw new FeaturegroupUpdateStatsError("Could not update statistics for trainingDataset:" +
        trainingDatasetDTO.getName() + " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
        + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
    return response;
  }
  
  /**
   * Makes a REST call to Hopsworks for synchronizing an existing Hive table with feature store metadata
   *
   * @param featuregroupDTO        the featurestore where the group will be created
   * @param featuregroupDTOType    the DTO type
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupCreationError FeaturegroupCreationError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   */
  public static void syncHiveTableWithFeaturestoreRest(FeaturegroupDTO featuregroupDTO, String featuregroupDTOType)
    throws JWTNotFoundException, JAXBException, FeaturegroupCreationError, FeaturestoreNotFound {
    LOG.log(Level.FINE, "Creating featuregroup " + featuregroupDTO.getName() +
      " in featurestore: " + featuregroupDTO.getFeaturestoreName());
    JSONObject json = FeaturestoreHelper.convertFeaturegroupDTOToJsonObject(featuregroupDTO);
    json.put(Constants.JSON_FEATURESTORE_ENTITY_TYPE, featuregroupDTOType);
    Response response;
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(featuregroupDTO.getFeaturestoreName());
      response = Hops.clientWrapper(json,
        "/project/" + Hops.getProjectId() + "/" + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE + "/" +
          featurestoreId + "/" + Constants.HOPSWORKS_REST_FEATUREGROUPS_RESOURCE + "/" +
          Constants.HOPSWORKS_REST_FEATUREGROUPS_SYNC_RESOURCE,
        HttpMethod.POST, null);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupCreationError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.CREATED.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = Hops.parseHopsworksErrorResponse(response);
      throw new FeaturegroupCreationError("Could not create featuregroup:" + featuregroupDTO.getName() +
        " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
        + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
  }
  
  /**
   * Makes a REST call to Hopsworks for enabling online serving of a feature group (create MySQL table)
   *
   * @param featuregroupDTO        DTO of the feature group
   * @param featuregroupDTOType    the DTO type
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupEnableOnlineError FeaturegroupEnableOnlineError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   */
  public static void enableFeaturegroupOnlineRest(
    FeaturegroupDTO featuregroupDTO, String featuregroupDTOType)
    throws JWTNotFoundException,
    JAXBException, FeaturestoreNotFound, FeaturegroupDoesNotExistError, FeaturegroupEnableOnlineError {
    LOG.log(Level.FINE,
      "Enabling online feature serving for feature group: " + featuregroupDTO.getName() +
        " in featurestore: " + featuregroupDTO.getFeaturestoreName());
    JSONObject json = FeaturestoreHelper.convertFeaturegroupDTOToJsonObject(featuregroupDTO);
    json.put(Constants.JSON_FEATURESTORE_ENTITY_TYPE, featuregroupDTOType);
    Response response;
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(featuregroupDTO.getFeaturestoreName());
      int featuregroupId = FeaturestoreHelper.getFeaturegroupId(featuregroupDTO.getFeaturestoreName(),
        featuregroupDTO.getName(), featuregroupDTO.getVersion());
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put(Constants.JSON_FEATURESTORE_ENABLE_ONLINE_QUERY_PARAM, true);
      queryParams.put(Constants.JSON_FEATURESTORE_DISABLE_ONLINE_QUERY_PARAM, false);
      queryParams.put(Constants.JSON_FEATURESTORE_UPDATE_STATS_QUERY_PARAM, false);
      queryParams.put(Constants.JSON_FEATURESTORE_UPDATE_JOB_QUERY_PARAM, !featuregroupDTO.getJobs().isEmpty());
      response = Hops.clientWrapper(json,
        "/project/" + Hops.getProjectId() + "/" + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE + "/" +
          featurestoreId + "/" + Constants.HOPSWORKS_REST_FEATUREGROUPS_RESOURCE + "/" + featuregroupId,
        HttpMethod.PUT, queryParams);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupEnableOnlineError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = Hops.parseHopsworksErrorResponse(response);
      LOG.severe("Could not enable online feature serving for featuregroup:" + featuregroupDTO.getName() +
        " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
        + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
      throw new FeaturegroupEnableOnlineError("Could not enable online feature serving for featuregroup:" +
        featuregroupDTO.getName() + " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
        + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
  }
  
  /**
   * Makes a REST call to Hopsworks for enabling online serving of a feature group (create MySQL table)
   *
   * @param featuregroupDTO        DTO of the feature group
   * @param featuregroupDTOType    the DTO type
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupDisableOnlineError FeaturegroupDisableOnlineError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   */
  public static void disableFeaturegroupOnlineRest(
    FeaturegroupDTO featuregroupDTO, String featuregroupDTOType) throws JWTNotFoundException,
    JAXBException, FeaturestoreNotFound, FeaturegroupDoesNotExistError, FeaturegroupDisableOnlineError {
    LOG.log(Level.FINE,
      "Enabling online feature serving for feature group: " + featuregroupDTO.getName() +
        " in featurestore: " + featuregroupDTO.getFeaturestoreName());
    JSONObject json = FeaturestoreHelper.convertFeaturegroupDTOToJsonObject(featuregroupDTO);
    json.put(Constants.JSON_FEATURESTORE_ENTITY_TYPE, featuregroupDTOType);
    Response response;
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(featuregroupDTO.getFeaturestoreName());
      int featuregroupId = FeaturestoreHelper.getFeaturegroupId(featuregroupDTO.getFeaturestoreName(),
        featuregroupDTO.getName(), featuregroupDTO.getVersion());
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put(Constants.JSON_FEATURESTORE_DISABLE_ONLINE_QUERY_PARAM, true);
      queryParams.put(Constants.JSON_FEATURESTORE_ENABLE_ONLINE_QUERY_PARAM, false);
      queryParams.put(Constants.JSON_FEATURESTORE_UPDATE_STATS_QUERY_PARAM, false);
      queryParams.put(Constants.JSON_FEATURESTORE_UPDATE_JOB_QUERY_PARAM, !featuregroupDTO.getJobs().isEmpty());
      response = Hops.clientWrapper(json,
        "/project/" + Hops.getProjectId() + "/" + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE + "/" +
          featurestoreId + "/" + Constants.HOPSWORKS_REST_FEATUREGROUPS_RESOURCE + "/" + featuregroupId,
        HttpMethod.PUT, queryParams);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupDisableOnlineError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = Hops.parseHopsworksErrorResponse(response);
      LOG.severe("Could not disable online feature serving for featuregroup:" + featuregroupDTO.getName() +
        " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
        + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
      throw new FeaturegroupDisableOnlineError("Could not disable online feature serving for featuregroup:" +
        featuregroupDTO.getName() + " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
        + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
  }
  
  /**
   * Makes a REST call to Hopsworks to get the JDBC connection to the online featurestore
   *
   * @param featurestore the featurestore to get the JDBC connection for the online featurestore
   * @return DTO of the JDBC connector to the online featurestore
   * @throws FeaturestoreNotFound FeaturestoresNotFound
   * @throws JAXBException JAXBException
   */
  public static FeaturestoreJdbcConnectorDTO getOnlineFeaturestoreJdbcConnectorRest(String featurestore)
    throws FeaturestoreNotFound, JAXBException {
    LOG.log(Level.FINE, "Getting JDBC connector for online feature store: " + featurestore);
    
    Response response;
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(featurestore);
      response =
        Hops.clientWrapper(
          Constants.SLASH_DELIMITER + Constants.HOPSWORKS_REST_PROJECT_RESOURCE
            + Constants.SLASH_DELIMITER + Hops.getProjectId()
            + Constants.SLASH_DELIMITER
            + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE + Constants.SLASH_DELIMITER +
            featurestoreId + Constants.SLASH_DELIMITER + Constants.HOPSWORKS_REST_STORAGE_CONNECTORS_RESOURCE,
          HttpMethod.GET, null);
    } catch (HTTPSClientInitializationException | JWTNotFoundException e) {
      throw new FeaturestoreNotFound(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      throw new FeaturestoreNotFound("Could not get JDBC Connector for online featurestore:" + featurestore);
    }
    final String responseEntity = response.readEntity(String.class);

    JSONArray connectors = new JSONArray(responseEntity);
    FeaturestoreJdbcConnectorDTO featurestoreJdbcConnectorDTO = null;
    for (int i = 0; i < connectors.length(); i++) {
      FeaturestoreJdbcConnectorDTO dto = FeaturestoreHelper.parseJdbcConnectorJson(connectors.getJSONObject(i));
      if(dto.getName().contains(Constants.ONLINE_JDBC_CONNECTOR_SUFFIX) &&
              dto.getStorageConnectorType() == FeaturestoreStorageConnectorType.JDBC) {
        featurestoreJdbcConnectorDTO = dto;
        break;
      }
    }
    if(featurestoreJdbcConnectorDTO == null) {
      throw new FeaturestoreNotFound("Could not find the online featurestore JDBC connector");
    }
    return featurestoreJdbcConnectorDTO;
  }
  
  /**
   * Makes a REST call to Hopsworks to attach tag to a featuregroup or training dataset
   *
   * @param name the featuregroup or training dataset name
   * @param featurestore the feature store
   * @param tag name of the tag to attach
   * @param value value of the tag
   * @param resource featuregroup or training dataset resource
   * @param id the id of the featuregroup or training dataset
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @throws TagError TagsError
   */
  public static void addTag(String name, String featurestore, String tag, String value, String resource, Integer id)
      throws FeaturestoreNotFound, JAXBException, TagError {
    LOG.log(Level.FINE,"Adding tag " + tag + " to " + name);
    
    Response response;
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(featurestore);
      Map<String, Object> queryParams = new HashMap<>();
      if(value != null) {
        queryParams.put(Constants.JSON_TAGS_VALUE_QUERY_PARAM, value);
      }
      response =
          Hops.clientWrapper(Constants.SLASH_DELIMITER +
                  Constants.HOPSWORKS_REST_PROJECT_RESOURCE
                  + Constants.SLASH_DELIMITER + Hops.getProjectId()
                  + Constants.SLASH_DELIMITER
                  + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE +
                  Constants.SLASH_DELIMITER +
                  featurestoreId + Constants.SLASH_DELIMITER +
                  resource +
                  Constants.SLASH_DELIMITER +
                  id +
                  Constants.SLASH_DELIMITER +
                  Constants.HOPSWORKS_FEATURESTORE_TAGS_RESOURCE +
                  Constants.SLASH_DELIMITER +
                  tag,
              HttpMethod.PUT, queryParams);
    } catch (HTTPSClientInitializationException | JWTNotFoundException e) {
      throw new FeaturestoreNotFound(e.getMessage());
    }
    LOG.log(Level.INFO,"******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() !=
        Response.Status.OK.getStatusCode()
        && response.getStatusInfo().getStatusCode() !=
        Response.Status.CREATED.getStatusCode()) {
      throw new TagError("Error while attaching tags to " + name + ", Http Code: " +
          response.getStatus());
    }
  }
  
  /**
   * Makes a REST call to Hopsworks to get tags attached to a
   * featuregroup or training dataset
   *
   * @param name the featuregroup or training dataset name
   * @param featurestore the feature store
   * @param resource featuregroup or training dataset resource
   * @param id the id of the featuregroup or training dataset
   * @return tags as map
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @throws TagError TagsError
   */
  public static Map<String, String> getTags(String name, String featurestore, String resource, Integer id)
      throws FeaturestoreNotFound, JAXBException, TagError {
    LOG.log(Level.FINE,"Getting tags for " + name);
    
    Response response;
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(featurestore);
      String path =
          Constants.SLASH_DELIMITER + Constants.HOPSWORKS_REST_PROJECT_RESOURCE
              + Constants.SLASH_DELIMITER + Hops.getProjectId()
              + Constants.SLASH_DELIMITER
              + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE +
              Constants.SLASH_DELIMITER +
              featurestoreId + Constants.SLASH_DELIMITER +
              resource +
              Constants.SLASH_DELIMITER +
              id +
              Constants.SLASH_DELIMITER +
              Constants.HOPSWORKS_FEATURESTORE_TAGS_RESOURCE;
      response = Hops.clientWrapper(path, HttpMethod.GET, null);
    } catch (HTTPSClientInitializationException | JWTNotFoundException e) {
      throw new FeaturestoreNotFound(e.getMessage());
    }
    LOG.log(Level.INFO,
        "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() !=
        Response.Status.OK.getStatusCode()) {
      throw new TagError("Error while getting tags for " + name + ", Http Code: " +
          response.getStatus());
    }
    final String responseEntity = response.readEntity(String.class);
    JSONObject tags = new JSONObject(responseEntity);
    Map<String, String> result = new HashMap<>();
    if(tags.has("items")) {
      JSONArray items = tags.getJSONArray("items");
      for (int i = 0; i < items.length(); i++) {
        JSONObject jsonObject = items.getJSONObject(i);
        result.put(jsonObject.getString("name"), jsonObject.getString("value"));
      }
    }
    return result;
  }
  
  /**
   * Makes a REST call to Hopsworks to remove tag attached to a
   * featuregroup or training dataset
   *
   * @param name the featuregroup or training dataset name
   * @param tag the name of the tag
   * @param featurestore the feature store
   * @param resource featuregroup or training dataset resource
   * @param id the id of the featuregroup or training dataset
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws JAXBException JAXBException
   * @throws TagError TagsError
   */
  public static void removeTag(String name, String tag, String featurestore, String resource, Integer id)
      throws FeaturestoreNotFound, JAXBException, TagError {
    LOG.log(Level.FINE,"Removing tag " + tag + " from " + name);
    
    Response response;
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(featurestore);
      response =
          Hops.clientWrapper(
              Constants.SLASH_DELIMITER +
                  Constants.HOPSWORKS_REST_PROJECT_RESOURCE
                  + Constants.SLASH_DELIMITER + Hops.getProjectId()
                  + Constants.SLASH_DELIMITER
                  + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE +
                  Constants.SLASH_DELIMITER +
                  featurestoreId + Constants.SLASH_DELIMITER +
                  resource +
                  Constants.SLASH_DELIMITER +
                  id +
                  Constants.SLASH_DELIMITER +
                  Constants.HOPSWORKS_FEATURESTORE_TAGS_RESOURCE +
                  Constants.SLASH_DELIMITER +
                  tag,
              HttpMethod.DELETE, null);
    } catch (HTTPSClientInitializationException | JWTNotFoundException e) {
      throw new FeaturestoreNotFound(e.getMessage());
    }
    LOG.log(Level.INFO,
        "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() !=
        Response.Status.NO_CONTENT.getStatusCode()) {
      throw new TagError("Error while removing tags from " + name
          + ", Http Code: " + response.getStatus());
    }
  }

  /**
   * Makes a REST call to Hopsworks to get tags that can be attached to a featuregroup or training dataset
   *
   * @return tags as list
   * @throws TagError TagsError
   */
  public static List<String> getFsTags() throws TagError {
    LOG.log(Level.FINE,"Getting featurestore tags");

    Response response;
    try {
      String path = Constants.SLASH_DELIMITER + Constants.HOPSWORKS_FEATURESTORE_TAGS_RESOURCE;
      response = Hops.clientWrapper(path, HttpMethod.GET, null);
    } catch (HTTPSClientInitializationException | JWTNotFoundException e) {
      throw new TagError(e.getMessage());
    }
    LOG.log(Level.INFO,
        "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() !=
        Response.Status.OK.getStatusCode()) {
      throw new TagError("Error while getting featurestore tags, Http Code: " +
          response.getStatus());
    }
    final String responseEntity = response.readEntity(String.class);
    JSONObject tags = new JSONObject(responseEntity);
    List<String> tagsList = new ArrayList<>();
    if(tags.has("items")) {
      JSONArray items = tags.getJSONArray("items");
      for (int i = 0; i < items.length(); i++) {
        JSONObject tag = items.getJSONObject(i);
        tagsList.add(tag.getString("name"));
      }
    }
    return tagsList;
  }
}

