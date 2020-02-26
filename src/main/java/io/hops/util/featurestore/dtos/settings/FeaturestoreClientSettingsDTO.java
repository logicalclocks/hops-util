/*
 * This file is part of Hopsworks
 * Copyright (C) 2018, Logical Clocks AB. All rights reserved
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

package io.hops.util.featurestore.dtos.settings;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * DTO containing the feature store client settings (source of truth for JS client, Python Client, Java Client, and
 * Scala Client to the Feature Store
 */
@XmlRootElement
public class FeaturestoreClientSettingsDTO {
  
  private int featurestoreStatisticsMaxCorrelations;
  private String featurestoreRegex;
  private int featurestoreEntityNameMaxLength;
  private int featurestoreEntityDescriptionMaxLength;
  private int storageConnectorNameMaxLength;
  private int storageConnectorDescriptionMaxLength;
  private int jdbcStorageConnectorConnectionstringMaxLength;
  private int jdbcStorageConnectorArgumentsMaxLength;
  private int s3StorageConnectorBucketMaxLength;
  private int s3StorageConnectorAccesskeyMaxLength;
  private int s3StorageConnectorSecretkeyMaxLength;
  private int onDemandFeaturegroupSqlQueryMaxLength;
  private List<String> trainingDatasetDataFormats;
  private String onDemandFeaturegroupType;
  private String cachedFeaturegroupType;
  private String jdbcConnectorType;
  private String hopsfsConnectorType;
  private String s3ConnectorType;
  private String cachedFeaturegroupDtoType;
  private String onDemandFeaturegroupDtoType;
  private String hopsfsTrainingDatasetType;
  private String externalTrainingDatasetType;
  private String s3ConnectorDtoType;
  private String jdbcConnectorDtoType;
  private String hopsfsConnectorDtoType;
  private String featuregroupType;
  private String trainingDatasetType;
  private List<String> suggestedFeatureTypes;
  private String featurestoreUtil4jMainClass;
  private String featurestoreUtil4jArgsDataset;
  private String featurestoreUtilPythonMainClass;
  private String featurestoreUtil4jExecutable;
  private String featurestoreUtilPythonExecutable;
  private String s3BucketTrainingDatasetsFolder;
  private List<String> featureImportConnectors;
  private Boolean onlineFeaturestoreEnabled = false;
  private List<String> suggestedMysqlFeatureTypes;
  
  
  public FeaturestoreClientSettingsDTO() {
    //For JAXB
  }
  
  @XmlElement
  public int getFeaturestoreStatisticsMaxCorrelations() {
    return featurestoreStatisticsMaxCorrelations;
  }
  
  public void setFeaturestoreStatisticsMaxCorrelations(int featurestoreStatisticsMaxCorrelations) {
    this.featurestoreStatisticsMaxCorrelations = featurestoreStatisticsMaxCorrelations;
  }
  
  @XmlElement
  public String getFeaturestoreRegex() {
    return featurestoreRegex;
  }
  
  public void setFeaturestoreRegex(String featurestoreRegex) {
    this.featurestoreRegex = featurestoreRegex;
  }
  
  @XmlElement
  public int getFeaturestoreEntityNameMaxLength() {
    return featurestoreEntityNameMaxLength;
  }
  
  public void setFeaturestoreEntityNameMaxLength(int featurestoreEntityNameMaxLength) {
    this.featurestoreEntityNameMaxLength = featurestoreEntityNameMaxLength;
  }
  
  @XmlElement
  public int getFeaturestoreEntityDescriptionMaxLength() {
    return featurestoreEntityDescriptionMaxLength;
  }
  
  public void setFeaturestoreEntityDescriptionMaxLength(int featurestoreEntityDescriptionMaxLength) {
    this.featurestoreEntityDescriptionMaxLength = featurestoreEntityDescriptionMaxLength;
  }
  
  @XmlElement
  public int getStorageConnectorNameMaxLength() {
    return storageConnectorNameMaxLength;
  }
  
  public void setStorageConnectorNameMaxLength(int storageConnectorNameMaxLength) {
    this.storageConnectorNameMaxLength = storageConnectorNameMaxLength;
  }
  
  @XmlElement
  public int getStorageConnectorDescriptionMaxLength() {
    return storageConnectorDescriptionMaxLength;
  }
  
  public void setStorageConnectorDescriptionMaxLength(int storageConnectorDescriptionMaxLength) {
    this.storageConnectorDescriptionMaxLength = storageConnectorDescriptionMaxLength;
  }
  
  @XmlElement
  public int getJdbcStorageConnectorConnectionstringMaxLength() {
    return jdbcStorageConnectorConnectionstringMaxLength;
  }
  
  public void setJdbcStorageConnectorConnectionstringMaxLength(int jdbcStorageConnectorConnectionstringMaxLength) {
    this.jdbcStorageConnectorConnectionstringMaxLength = jdbcStorageConnectorConnectionstringMaxLength;
  }
  
  @XmlElement
  public int getJdbcStorageConnectorArgumentsMaxLength() {
    return jdbcStorageConnectorArgumentsMaxLength;
  }
  
  public void setJdbcStorageConnectorArgumentsMaxLength(int jdbcStorageConnectorArgumentsMaxLength) {
    this.jdbcStorageConnectorArgumentsMaxLength = jdbcStorageConnectorArgumentsMaxLength;
  }
  
  @XmlElement
  public int getS3StorageConnectorBucketMaxLength() {
    return s3StorageConnectorBucketMaxLength;
  }
  
  public void setS3StorageConnectorBucketMaxLength(int s3StorageConnectorBucketMaxLength) {
    this.s3StorageConnectorBucketMaxLength = s3StorageConnectorBucketMaxLength;
  }
  
  @XmlElement
  public int getS3StorageConnectorAccesskeyMaxLength() {
    return s3StorageConnectorAccesskeyMaxLength;
  }
  
  public void setS3StorageConnectorAccesskeyMaxLength(int s3StorageConnectorAccesskeyMaxLength) {
    this.s3StorageConnectorAccesskeyMaxLength = s3StorageConnectorAccesskeyMaxLength;
  }
  
  @XmlElement
  public int getS3StorageConnectorSecretkeyMaxLength() {
    return s3StorageConnectorSecretkeyMaxLength;
  }
  
  public void setS3StorageConnectorSecretkeyMaxLength(int s3StorageConnectorSecretkeyMaxLength) {
    this.s3StorageConnectorSecretkeyMaxLength = s3StorageConnectorSecretkeyMaxLength;
  }
  
  @XmlElement
  public int getOnDemandFeaturegroupSqlQueryMaxLength() {
    return onDemandFeaturegroupSqlQueryMaxLength;
  }
  
  public void setOnDemandFeaturegroupSqlQueryMaxLength(int onDemandFeaturegroupSqlQueryMaxLength) {
    this.onDemandFeaturegroupSqlQueryMaxLength = onDemandFeaturegroupSqlQueryMaxLength;
  }
  
  @XmlElement
  public List<String> getTrainingDatasetDataFormats() {
    return trainingDatasetDataFormats;
  }
  
  public void setTrainingDatasetDataFormats(List<String> trainingDatasetDataFormats) {
    this.trainingDatasetDataFormats = trainingDatasetDataFormats;
  }
  
  @XmlElement
  public String getOnDemandFeaturegroupType() {
    return onDemandFeaturegroupType;
  }
  
  public void setOnDemandFeaturegroupType(String onDemandFeaturegroupType) {
    this.onDemandFeaturegroupType = onDemandFeaturegroupType;
  }
  
  @XmlElement
  public String getCachedFeaturegroupType() {
    return cachedFeaturegroupType;
  }
  
  public void setCachedFeaturegroupType(String cachedFeaturegroupType) {
    this.cachedFeaturegroupType = cachedFeaturegroupType;
  }
  
  @XmlElement
  public String getJdbcConnectorType() {
    return jdbcConnectorType;
  }
  
  public void setJdbcConnectorType(String jdbcConnectorType) {
    this.jdbcConnectorType = jdbcConnectorType;
  }
  
  @XmlElement
  public String getHopsfsConnectorType() {
    return hopsfsConnectorType;
  }
  
  public void setHopsfsConnectorType(String hopsfsConnectorType) {
    this.hopsfsConnectorType = hopsfsConnectorType;
  }
  
  @XmlElement
  public String getS3ConnectorType() {
    return s3ConnectorType;
  }
  
  public void setS3ConnectorType(String s3ConnectorType) {
    this.s3ConnectorType = s3ConnectorType;
  }
  
  @XmlElement
  public String getCachedFeaturegroupDtoType() {
    return cachedFeaturegroupDtoType;
  }
  
  public void setCachedFeaturegroupDtoType(String cachedFeaturegroupDtoType) {
    this.cachedFeaturegroupDtoType = cachedFeaturegroupDtoType;
  }
  
  @XmlElement
  public String getOnDemandFeaturegroupDtoType() {
    return onDemandFeaturegroupDtoType;
  }
  
  public void setOnDemandFeaturegroupDtoType(String onDemandFeaturegroupDtoType) {
    this.onDemandFeaturegroupDtoType = onDemandFeaturegroupDtoType;
  }
  
  @XmlElement
  public String getHopsfsTrainingDatasetType() {
    return hopsfsTrainingDatasetType;
  }
  
  public void setHopsfsTrainingDatasetType(String hopsfsTrainingDatasetType) {
    this.hopsfsTrainingDatasetType = hopsfsTrainingDatasetType;
  }
  
  @XmlElement
  public String getExternalTrainingDatasetType() {
    return externalTrainingDatasetType;
  }
  
  public void setExternalTrainingDatasetType(String externalTrainingDatasetType) {
    this.externalTrainingDatasetType = externalTrainingDatasetType;
  }

  @XmlElement
  public String getS3ConnectorDtoType() {
    return s3ConnectorDtoType;
  }
  
  public void setS3ConnectorDtoType(String s3ConnectorDtoType) {
    this.s3ConnectorDtoType = s3ConnectorDtoType;
  }
  
  @XmlElement
  public String getJdbcConnectorDtoType() {
    return jdbcConnectorDtoType;
  }
  
  public void setJdbcConnectorDtoType(String jdbcConnectorDtoType) {
    this.jdbcConnectorDtoType = jdbcConnectorDtoType;
  }
  
  @XmlElement
  public String getHopsfsConnectorDtoType() {
    return hopsfsConnectorDtoType;
  }
  
  public void setHopsfsConnectorDtoType(String hopsfsConnectorDtoType) {
    this.hopsfsConnectorDtoType = hopsfsConnectorDtoType;
  }
  
  @XmlElement
  public String getFeaturegroupType() {
    return featuregroupType;
  }
  
  public void setFeaturegroupType(String featuregroupType) {
    this.featuregroupType = featuregroupType;
  }
  
  @XmlElement
  public String getTrainingDatasetType() {
    return trainingDatasetType;
  }
  
  public void setTrainingDatasetType(String trainingDatasetType) {
    this.trainingDatasetType = trainingDatasetType;
  }
  
  @XmlElement
  public List<String> getSuggestedFeatureTypes() {
    return suggestedFeatureTypes;
  }
  
  public void setSuggestedFeatureTypes(List<String> suggestedFeatureTypes) {
    this.suggestedFeatureTypes = suggestedFeatureTypes;
  }
  
  @XmlElement
  public String getFeaturestoreUtil4jMainClass() {
    return featurestoreUtil4jMainClass;
  }
  
  public void setFeaturestoreUtil4jMainClass(String featurestoreUtil4jMainClass) {
    this.featurestoreUtil4jMainClass = featurestoreUtil4jMainClass;
  }
  
  @XmlElement
  public String getFeaturestoreUtil4jArgsDataset() {
    return featurestoreUtil4jArgsDataset;
  }
  
  public void setFeaturestoreUtil4jArgsDataset(String featurestoreUtil4jArgsDataset) {
    this.featurestoreUtil4jArgsDataset = featurestoreUtil4jArgsDataset;
  }
  
  @XmlElement
  public String getFeaturestoreUtilPythonMainClass() {
    return featurestoreUtilPythonMainClass;
  }
  
  public void setFeaturestoreUtilPythonMainClass(String featurestoreUtilPythonMainClass) {
    this.featurestoreUtilPythonMainClass = featurestoreUtilPythonMainClass;
  }
  
  @XmlElement
  public String getFeaturestoreUtil4jExecutable() {
    return featurestoreUtil4jExecutable;
  }
  
  public void setFeaturestoreUtil4jExecutable(String featurestoreUtil4jExecutable) {
    this.featurestoreUtil4jExecutable = featurestoreUtil4jExecutable;
  }
  
  @XmlElement
  public String getFeaturestoreUtilPythonExecutable() {
    return featurestoreUtilPythonExecutable;
  }
  
  public void setFeaturestoreUtilPythonExecutable(String featurestoreUtilPythonExecutable) {
    this.featurestoreUtilPythonExecutable = featurestoreUtilPythonExecutable;
  }
  
  @XmlElement
  public String getS3BucketTrainingDatasetsFolder() {
    return s3BucketTrainingDatasetsFolder;
  }
  
  public void setS3BucketTrainingDatasetsFolder(String s3BucketTrainingDatasetsFolder) {
    this.s3BucketTrainingDatasetsFolder = s3BucketTrainingDatasetsFolder;
  }
  
  @XmlElement
  public List<String> getFeatureImportConnectors() {
    return featureImportConnectors;
  }
  
  public void setFeatureImportConnectors(List<String> featureImportConnectors) {
    this.featureImportConnectors = featureImportConnectors;
  }
  
  @XmlElement
  public Boolean getOnlineFeaturestoreEnabled() {
    return onlineFeaturestoreEnabled;
  }
  
  public void setOnlineFeaturestoreEnabled(Boolean onlineFeaturestoreEnabled) {
    this.onlineFeaturestoreEnabled = onlineFeaturestoreEnabled;
  }
  
  @XmlElement
  public List<String> getSuggestedMysqlFeatureTypes() {
    return suggestedMysqlFeatureTypes;
  }
  
  public void setSuggestedMysqlFeatureTypes(List<String> suggestedMysqlFeatureTypes) {
    this.suggestedMysqlFeatureTypes = suggestedMysqlFeatureTypes;
  }
}
