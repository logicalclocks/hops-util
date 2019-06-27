package io.hops.util;

import io.hops.util.exceptions.FeaturegroupCreationError;
import io.hops.util.exceptions.FeaturegroupDeletionError;
import io.hops.util.exceptions.FeaturegroupDoesNotExistError;
import io.hops.util.exceptions.FeaturegroupUpdateStatsError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.FeaturestoresNotFound;
import io.hops.util.exceptions.HTTPSClientInitializationException;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.exceptions.TrainingDatasetCreationError;
import io.hops.util.exceptions.TrainingDatasetDoesNotExistError;
import io.hops.util.featurestore.dtos.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.FeatureDTO;
import io.hops.util.featurestore.dtos.stats.StatisticsDTO;
import org.json.JSONObject;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;
import java.util.List;
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
    LOG.log(Level.FINE, "Getting featuregroups for featurestore " + featurestore);
    
    Response response;
    try {
      response =
        Hops.clientWrapper(
          "/project/" + Hops.getProjectId() + "/" + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE + "/" +
            featurestore + "/" + Constants.HOPSWORKS_REST_FEATURESTORE_METADATA_RESOURCE,
          HttpMethod.GET);
    } catch (HTTPSClientInitializationException | JWTNotFoundException e) {
      throw new FeaturestoreNotFound(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      throw new FeaturestoreNotFound("Could not fetch featuregroups for featurestore:" + featurestore);
    }
    final String responseEntity = response.readEntity(String.class);
    
    JSONObject featurestoreMetadata = new JSONObject(responseEntity);
    return FeaturestoreHelper.parseFeaturestoreMetadataJson(featurestoreMetadata);
  }
  
  /**
   * Makes a REST call to Hopsworks for deleting
   * the contents of the featuregroup but keeps the featuregroup metadata
   *
   * @param featurestore        the featurestore where the featuregroup resides
   * @param featuregroup        the featuregroup to drop the contents of
   * @param featuregroupVersion the version of the featurergroup
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws FeaturegroupDeletionError FeaturegroupDeletionError
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   */
  public static void deleteTableContentsRest(
    String featurestore, String featuregroup, int featuregroupVersion)
    throws JWTNotFoundException, FeaturegroupDeletionError, JAXBException, FeaturestoreNotFound,
    FeaturegroupDoesNotExistError {
    LOG.log(Level.FINE, "Deleting table contents of featuregroup " + featuregroup +
      "version: " + featuregroupVersion + " in featurestore: " + featurestore);
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(featurestore);
      int featuregroupId = FeaturestoreHelper.getFeaturegroupId(featurestore, featuregroup, featuregroupVersion);
      Response response = Hops.clientWrapper(
        "/project/" + Hops.getProjectId() + "/" + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE + "/" +
          featurestoreId + "/" + Constants.HOPSWORKS_REST_FEATUREGROUPS_RESOURCE + "/" + featuregroupId + "/" +
          Constants.HOPSWORKS_REST_FEATUREGROUP_CLEAR_RESOURCE, HttpMethod.POST);
      LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
      if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
        throw new FeaturegroupDeletionError("Could not clear the contents of featuregroup:" + featuregroup +
          " , response code: " + response.getStatusInfo().getStatusCode());
      }
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupDeletionError(e.getMessage());
    }
  }
  
  /**
   * Makes a REST call to Hopsworks for creating a new featuregroup from a spark dataframe.
   *
   * @param featurestore        the featurestore where the group will be created
   * @param featuregroup        the name of the featuregroup
   * @param featuregroupVersion the version of the featuregroup
   * @param description         the description of the featuregroup
   * @param jobName               the name of the job to compute the featuregroup
   * @param featuresSchema      schema of features for the featuregroup
   * @param statisticsDTO       statistics about the featuregroup
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupCreationError FeaturegroupCreationError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   */
  public static void createFeaturegroupRest(
    String featurestore, String featuregroup, int featuregroupVersion, String description,
    String jobName, List<FeatureDTO> featuresSchema,
    StatisticsDTO statisticsDTO)
    throws JWTNotFoundException, JAXBException, FeaturegroupCreationError, FeaturestoreNotFound {
    LOG.log(Level.FINE, "Creating featuregroup " + featuregroup + " in featurestore: " + featurestore);
    JSONObject json = new JSONObject();
    json.put(Constants.JSON_FEATUREGROUP_NAME, featuregroup);
    json.put(Constants.JSON_FEATUREGROUP_VERSION, featuregroupVersion);
    json.put(Constants.JSON_FEATUREGROUP_DESCRIPTION, description);
    json.put(Constants.JSON_FEATUREGROUP_JOBNAME, jobName);
    json.put(Constants.JSON_FEATUREGROUP_FEATURES,
      FeaturestoreHelper.convertFeatureDTOsToJsonObjects(featuresSchema));
    json.put(Constants.JSON_FEATUREGROUP_FEATURE_CORRELATION,
      FeaturestoreHelper.convertFeatureCorrelationMatrixDTOToJsonObject(
        statisticsDTO.getFeatureCorrelationMatrixDTO()));
    json.put(Constants.JSON_FEATUREGROUP_DESC_STATS,
      FeaturestoreHelper.convertDescriptiveStatsDTOToJsonObject(statisticsDTO.getDescriptiveStatsDTO()));
    json.put(Constants.JSON_FEATUREGROUP_FEATURES_HISTOGRAM, FeaturestoreHelper
      .convertFeatureDistributionsDTOToJsonObject(statisticsDTO.getFeatureDistributionsDTO()));
    json.put(Constants.JSON_FEATUREGROUP_CLUSTER_ANALYSIS, FeaturestoreHelper
      .convertClusterAnalysisDTOToJsonObject(statisticsDTO.getClusterAnalysisDTO()));
    json.put(Constants.JSON_FEATUREGROUP_UPDATE_METADATA, false);
    json.put(Constants.JSON_FEATUREGROUP_UPDATE_STATS, false);
    Response response;
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(featurestore);
      response = Hops.clientWrapper(json,
        "/project/" + Hops.getProjectId() + "/" + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE + "/" +
          featurestoreId + "/" + Constants.HOPSWORKS_REST_FEATUREGROUPS_RESOURCE,
        HttpMethod.POST);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupCreationError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.CREATED.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = Hops.parseHopsworksErrorResponse(response);
      throw new FeaturegroupCreationError("Could not create featuregroup:" + featuregroup +
        " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
        + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
  }
  
  /**
   * Makes a REST call to Hopsworks for creating a new training dataset from a spark dataframe
   *
   * @param featurestore           the featurestore where the group will be created
   * @param trainingDataset        the name of the training dataset to create
   * @param trainingDatasetVersion the version of the training dataset
   * @param description            the description of the training dataset
   * @param jobName                the name of the job to compute the training dataset
   * @param featuresSchema         schema of features for the training dataset
   * @param statisticsDTO          statistics about the featuregroup
   * @param dataFormat             format of the dataset (e.g tfrecords)
   * @return the JSON response
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws JAXBException JAXBException
   * @throws TrainingDatasetCreationError TrainingDatasetCreationError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   */
  public static Response createTrainingDatasetRest(
    String featurestore, String trainingDataset, int trainingDatasetVersion, String description,
    String jobName, String dataFormat, List<FeatureDTO> featuresSchema,
    StatisticsDTO statisticsDTO)
    throws JWTNotFoundException, JAXBException, TrainingDatasetCreationError, FeaturestoreNotFound {
    LOG.log(Level.FINE, "Creating Training Dataset " + trainingDataset + " in featurestore: " + featurestore);
    JSONObject json = new JSONObject();
    json.put(Constants.JSON_TRAINING_DATASET_NAME, trainingDataset);
    json.put(Constants.JSON_TRAINING_DATASET_VERSION, trainingDatasetVersion);
    json.put(Constants.JSON_TRAINING_DATASET_DESCRIPTION, description);
    json.put(Constants.JSON_TRAINING_DATASET_JOBNAME, jobName);
    json.put(Constants.JSON_TRAINING_DATASET_FORMAT, dataFormat);
    json.put(Constants.JSON_TRAINING_DATASET_SCHEMA,
      FeaturestoreHelper.convertFeatureDTOsToJsonObjects(featuresSchema));
    json.put(Constants.JSON_TRAINING_DATASET_FEATURE_CORRELATION,
      FeaturestoreHelper.convertFeatureCorrelationMatrixDTOToJsonObject(
        statisticsDTO.getFeatureCorrelationMatrixDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_DESC_STATS,
      FeaturestoreHelper.convertDescriptiveStatsDTOToJsonObject(statisticsDTO.getDescriptiveStatsDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_FEATURES_HISTOGRAM, FeaturestoreHelper
      .convertFeatureDistributionsDTOToJsonObject(statisticsDTO.getFeatureDistributionsDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_CLUSTER_ANALYSIS, FeaturestoreHelper
      .convertClusterAnalysisDTOToJsonObject(statisticsDTO.getClusterAnalysisDTO()));
    Response response;
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(featurestore);
      response = Hops.clientWrapper(json,
        "/project/" + Hops.getProjectId() + "/" + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE + "/" +
          featurestoreId + "/" + Constants.HOPSWORKS_REST_TRAININGDATASETS_RESOURCE,
        HttpMethod.POST);
    } catch (HTTPSClientInitializationException e) {
      throw new TrainingDatasetCreationError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.CREATED.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = Hops.parseHopsworksErrorResponse(response);
      throw new TrainingDatasetCreationError("Could not create trainingDataset:" + trainingDataset +
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
            Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE, HttpMethod.GET);
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
   * @param featuregroup        the name of the featuregroup
   * @param featurestore        the name of the featurestore where the featuregroup resides
   * @param featuregroupVersion the version of the featuregroup
   * @param statisticsDTO       the new statistics of the featuregroup
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws FeaturegroupDoesNotExistError FeaturegroupDoesNotExistError
   */
  public static void updateFeaturegroupStatsRest(
    String featuregroup, String featurestore, int featuregroupVersion, StatisticsDTO statisticsDTO)
    throws JWTNotFoundException,
    JAXBException, FeaturegroupUpdateStatsError, FeaturestoreNotFound, FeaturegroupDoesNotExistError {
    LOG.log(Level.FINE, "Updating featuregroup stats for: " + featuregroup + " in featurestore: " + featurestore);
    JSONObject json = new JSONObject();
    json.put(Constants.JSON_FEATUREGROUP_NAME, featuregroup);
    json.put(Constants.JSON_FEATUREGROUP_VERSION, featuregroupVersion);
    json.put(Constants.JSON_FEATUREGROUP_FEATURE_CORRELATION,
      FeaturestoreHelper.convertFeatureCorrelationMatrixDTOToJsonObject(
        statisticsDTO.getFeatureCorrelationMatrixDTO()));
    json.put(Constants.JSON_FEATUREGROUP_DESC_STATS,
      FeaturestoreHelper.convertDescriptiveStatsDTOToJsonObject(statisticsDTO.getDescriptiveStatsDTO()));
    json.put(Constants.JSON_FEATUREGROUP_FEATURES_HISTOGRAM, FeaturestoreHelper
      .convertFeatureDistributionsDTOToJsonObject(statisticsDTO.getFeatureDistributionsDTO()));
    json.put(Constants.JSON_FEATUREGROUP_CLUSTER_ANALYSIS, FeaturestoreHelper
      .convertClusterAnalysisDTOToJsonObject(statisticsDTO.getClusterAnalysisDTO()));
    json.put(Constants.JSON_FEATUREGROUP_UPDATE_METADATA, false);
    json.put(Constants.JSON_FEATUREGROUP_UPDATE_STATS, true);
    Response response;
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(featurestore);
      int featuregroupId = FeaturestoreHelper.getFeaturegroupId(featurestore, featuregroup, featuregroupVersion);
      response = Hops.clientWrapper(json,
        "/project/" + Hops.getProjectId() + "/" + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE + "/" +
          featurestoreId + "/" + Constants.HOPSWORKS_REST_FEATUREGROUPS_RESOURCE + "/" + featuregroupId,
        HttpMethod.PUT);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupUpdateStatsError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = Hops.parseHopsworksErrorResponse(response);
      throw new FeaturegroupUpdateStatsError("Could not update statistics for featuregroup:" + featuregroup +
        " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
        + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
  }
  
  /**
   * @param trainingDataset        the name of the training dataset
   * @param featurestore           the name of the featurestore where the training dataset resides
   * @param trainingDatasetVersion the version of the training dataset
   * @param statisticsDTO          the new statistics of the training dataset
   * @return the JSON response
   * @throws JWTNotFoundException JWTNotFoundException
   * @throws JAXBException JAXBException
   * @throws FeaturegroupUpdateStatsError FeaturegroupUpdateStatsError
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   * @throws TrainingDatasetDoesNotExistError TrainingDatasetDoesNotExistError
   */
  public static Response updateTrainingDatasetStatsRest(
    String trainingDataset, String featurestore, int trainingDatasetVersion, StatisticsDTO statisticsDTO)
    throws JWTNotFoundException,
    JAXBException, FeaturegroupUpdateStatsError, TrainingDatasetDoesNotExistError, FeaturestoreNotFound {
    LOG.log(Level.FINE, "Updating training dataset stats for: " + trainingDataset +
      " in featurestore: " + featurestore);
    JSONObject json = new JSONObject();
    json.put(Constants.JSON_TRAINING_DATASET_NAME, trainingDataset);
    json.put(Constants.JSON_TRAINING_DATASET_VERSION, trainingDatasetVersion);
    json.put(Constants.JSON_TRAINING_DATASET_FEATURE_CORRELATION,
      FeaturestoreHelper.convertFeatureCorrelationMatrixDTOToJsonObject(
        statisticsDTO.getFeatureCorrelationMatrixDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_DESC_STATS,
      FeaturestoreHelper.convertDescriptiveStatsDTOToJsonObject(statisticsDTO.getDescriptiveStatsDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_FEATURES_HISTOGRAM, FeaturestoreHelper
      .convertFeatureDistributionsDTOToJsonObject(statisticsDTO.getFeatureDistributionsDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_CLUSTER_ANALYSIS, FeaturestoreHelper
      .convertClusterAnalysisDTOToJsonObject(statisticsDTO.getClusterAnalysisDTO()));
    json.put(Constants.JSON_TRAINING_DATASET_UPDATE_METADATA, false);
    json.put(Constants.JSON_FEATUREGROUP_UPDATE_STATS, true);
    Response response = null;
    try {
      int featurestoreId = FeaturestoreHelper.getFeaturestoreId(featurestore);
      int trainingDatasetId = FeaturestoreHelper.getTrainingDatasetId(featurestore, trainingDataset,
        trainingDatasetVersion);
      response = Hops.clientWrapper(json,
        "/project/" + Hops.getProjectId() + "/" + Constants.HOPSWORKS_REST_FEATURESTORES_RESOURCE + "/" +
          featurestoreId + "/" + Constants.HOPSWORKS_REST_TRAININGDATASETS_RESOURCE + "/" + trainingDatasetId,
        HttpMethod.PUT);
    } catch (HTTPSClientInitializationException e) {
      throw new FeaturegroupUpdateStatsError(e.getMessage());
    }
    LOG.log(Level.INFO, "******* response.getStatusInfo():" + response.getStatusInfo());
    if (response.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      HopsworksErrorResponseDTO hopsworksErrorResponseDTO = Hops.parseHopsworksErrorResponse(response);
      throw new FeaturegroupUpdateStatsError("Could not update statistics for trainingDataset:" + trainingDataset +
        " , error code: " + hopsworksErrorResponseDTO.getErrorCode() + " error message: "
        + hopsworksErrorResponseDTO.getErrorMsg() + ", user message: " + hopsworksErrorResponseDTO.getUserMsg());
    }
    return response;
  }
}

