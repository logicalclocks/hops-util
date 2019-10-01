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
import io.hops.util.exceptions.TrainingDatasetCreationError;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreJdbcConnectorDTO;
import io.hops.util.featurestore.dtos.trainingdataset.ExternalTrainingDatasetDTO;
import io.hops.util.featurestore.dtos.trainingdataset.HopsfsTrainingDatasetDTO;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetDTO;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetType;
import org.json.JSONObject;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;
import java.util.HashMap;
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
   * @param trainingDatasetDTOType        the DTO type to send
   * @return the JSON response
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws JAXBException JAXBException
   * @throws TrainingDatasetCreationError TrainingDatasetCreationError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   */
  public static Response createTrainingDatasetRest(TrainingDatasetDTO trainingDatasetDTO, String trainingDatasetDTOType)
    throws JWTNotFoundException, JAXBException, TrainingDatasetCreationError, FeaturestoreNotFound {
    LOG.log(Level.FINE, "Creating Training Dataset " + trainingDatasetDTO.getName() +
      " in featurestore: " + trainingDatasetDTO.getFeaturestoreName());
    JSONObject json = FeaturestoreHelper.convertTrainingDatasetDTOToJsonObject(trainingDatasetDTO);
    json.put(Constants.JSON_FEATURESTORE_ENTITY_TYPE, trainingDatasetDTOType);
    Response response;
    if(trainingDatasetDTO.getTrainingDatasetType() == TrainingDatasetType.EXTERNAL_TRAINING_DATASET) {
      json.put(Constants.JSON_TRAINING_DATASET_S3_CONNECTOR_ID,
        ((ExternalTrainingDatasetDTO) trainingDatasetDTO).getS3ConnectorId());
    } else {
      json.put(Constants.JSON_TRAINING_DATASET_HOPSFS_CONNECTOR_ID,
        ((HopsfsTrainingDatasetDTO) trainingDatasetDTO).getHopsfsConnectorId());
    }
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
   * Makes a REST call to Hopsworks for updating the statistics of a featuregroup
   *
   * @param featuregroupDTO        DTO of the feature group
   * @param featuregroupDTOType    the DTO type
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   */
  public static void updateFeaturegroupStatsRest(
    FeaturegroupDTO featuregroupDTO, String featuregroupDTOType)
    throws JWTNotFoundException,
    JAXBException, FeaturegroupUpdateStatsError, FeaturestoreNotFound, FeaturegroupDoesNotExistError {
    LOG.log(Level.FINE,
      "Updating featuregroup stats for: " + featuregroupDTO.getName() +
        " in featurestore: " + featuregroupDTO.getFeaturestoreName());
    JSONObject json = FeaturestoreHelper.convertFeaturegroupDTOToJsonObject(featuregroupDTO);
    json.put(Constants.JSON_FEATURESTORE_ENTITY_TYPE, featuregroupDTOType);
    Response response;
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(featuregroupDTO.getFeaturestoreName());
      int featuregroupId = FeaturestoreHelper.getFeaturegroupId(featuregroupDTO.getFeaturestoreName(),
        featuregroupDTO.getName(), featuregroupDTO.getVersion());
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put(Constants.JSON_FEATURESTORE_UPDATE_STATS_QUERY_PARAM, true);
      if(!featuregroupDTO.getJobs().isEmpty()) {
        queryParams.put(Constants.JSON_FEATURESTORE_UPDATE_METADATA_QUERY_PARAM, true);
      } else {
        queryParams.put(Constants.JSON_FEATURESTORE_UPDATE_METADATA_QUERY_PARAM, false);
      }
      response = Hops.clientWrapper(json,
        "/project/" + Hops.getProjectId() + "/" + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE + "/" +
          featurestoreId + "/" + Constants.HOPSWORKS_REST_FEATUREGROUPS_RESOURCE + "/" + featuregroupId,
          HttpMethod.PUT, queryParams);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupUpdateStatsError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = Hops.parseHopsworksErrorResponse(response);
      LOG.severe("Could not update statistics for featuregroup:" + featuregroupDTO.getName() +
          " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
          + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
      throw new FeaturegroupUpdateStatsError("Could not update statistics for featuregroup:" +
        featuregroupDTO.getName() + " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
        + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
  }
  
  /**
   * @param trainingDatasetDTO        DTO of the training dataset
   * @param trainingDatasetDTOType    type of the DTO (sub-class info)
   * @return the JSON response
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   */
  public static Response updateTrainingDatasetStatsRest(
    TrainingDatasetDTO trainingDatasetDTO, String trainingDatasetDTOType)
    throws JWTNotFoundException,
    JAXBException, FeaturegroupUpdateStatsError, TrainingDatasetDoesNotExistError, FeaturestoreNotFound {
    LOG.log(Level.FINE, "Updating training dataset stats for: " + trainingDatasetDTO.getName() +
      " in featurestore: " + trainingDatasetDTO.getFeaturestoreName());
    JSONObject json = FeaturestoreHelper.convertTrainingDatasetDTOToJsonObject(trainingDatasetDTO);
    json.put(Constants.JSON_FEATURESTORE_ENTITY_TYPE, trainingDatasetDTOType);
    Response response = null;
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(trainingDatasetDTO.getFeaturestoreName());
      int trainingDatasetId = FeaturestoreHelper.getTrainingDatasetId(trainingDatasetDTO.getFeaturestoreName(),
        trainingDatasetDTO.getName(), trainingDatasetDTO.getVersion());
      Map<String, Object> queryParams = new HashMap<>();
      queryParams.put(Constants.JSON_FEATURESTORE_UPDATE_STATS_QUERY_PARAM, true);
      if(!trainingDatasetDTO.getJobs().isEmpty()){
        queryParams.put(Constants.JSON_FEATURESTORE_UPDATE_METADATA_QUERY_PARAM, true);
      } else {
        queryParams.put(Constants.JSON_FEATURESTORE_UPDATE_METADATA_QUERY_PARAM, false);
      }
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
      if(!featuregroupDTO.getJobs().isEmpty()) {
        queryParams.put(Constants.JSON_FEATURESTORE_UPDATE_METADATA_QUERY_PARAM, true);
      } else {
        queryParams.put(Constants.JSON_FEATURESTORE_UPDATE_METADATA_QUERY_PARAM, false);
      }
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
      if(!featuregroupDTO.getJobs().isEmpty()) {
        queryParams.put(Constants.JSON_FEATURESTORE_UPDATE_METADATA_QUERY_PARAM, true);
      } else {
        queryParams.put(Constants.JSON_FEATURESTORE_UPDATE_METADATA_QUERY_PARAM, false);
      }
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
            featurestoreId + Constants.SLASH_DELIMITER + Constants.HOPSWORKS_REST_STORAGE_CONNECTORS_RESOURCE +
            Constants.SLASH_DELIMITER + Constants.HOPSWORKS_ONLINE_FEATURESTORE_STORAGE_CONNECTOR_RESOURCE,
          HttpMethod.GET, null);
    } catch (HTTPSClientInitializationException | JWTNotFoundException e) {
      throw new FeaturestoreNotFound(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      throw new FeaturestoreNotFound("Could not get JDBC Connector for online featurestore:" + featurestore);
    }
    final String responseEntity = response.readEntity(String.class);
    
    JSONObject onlineFeaturestoreConnector = new JSONObject(responseEntity);
    return FeaturestoreHelper.parseJdbcConnectorJson(onlineFeaturestoreConnector);
  }
}

