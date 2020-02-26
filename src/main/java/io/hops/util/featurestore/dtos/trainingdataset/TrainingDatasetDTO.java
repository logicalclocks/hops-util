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

package io.hops.util.featurestore.dtos.trainingdataset;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.hops.util.featurestore.dtos.FeaturestoreEntityDTO;
import io.hops.util.featurestore.dtos.storageconnector.FeaturestoreStorageConnectorType;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * DTO containing the human-readable information of a trainingDataset, can be converted to JSON or XML representation
 * using jaxb.
 */
@XmlRootElement
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
public class TrainingDatasetDTO extends FeaturestoreEntityDTO {
  
  private String dataFormat;
  private TrainingDatasetType trainingDatasetType;

  private Integer storageConnectorId;
  private String storageConnectorName;
  private FeaturestoreStorageConnectorType storageConnectorType;
  private Long inodeId;

  public TrainingDatasetDTO() {
  }

  public TrainingDatasetDTO(String dataFormat, TrainingDatasetType trainingDatasetType) {
    this.dataFormat = dataFormat;
    this.trainingDatasetType = trainingDatasetType;
  }

  @XmlElement
  public String getDataFormat() {
    return dataFormat;
  }
  
  public void setDataFormat(String dataFormat) {
    this.dataFormat = dataFormat;
  }
  
  @XmlElement
  public TrainingDatasetType getTrainingDatasetType() {
    return trainingDatasetType;
  }
  
  public void setTrainingDatasetType(
    TrainingDatasetType trainingDatasetType) {
    this.trainingDatasetType = trainingDatasetType;
  }

  public Integer getStorageConnectorId() {
    return storageConnectorId;
  }

  public void setStorageConnectorId(Integer storageConnectorId) {
    this.storageConnectorId = storageConnectorId;
  }

  public String getStorageConnectorName() {
    return storageConnectorName;
  }

  public void setStorageConnectorName(String storageConnectorName) {
    this.storageConnectorName = storageConnectorName;
  }

  public FeaturestoreStorageConnectorType getStorageConnectorType() {
    return storageConnectorType;
  }

  public void setStorageConnectorType(FeaturestoreStorageConnectorType storageConnectorType) {
    this.storageConnectorType = storageConnectorType;
  }

  public Long getInodeId() {
    return inodeId;
  }

  public void setInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }

  @Override
  public String toString() {
    return "TrainingDatasetDTO{" +
      "dataFormat='" + dataFormat + '\'' +
      ", trainingDatasetType=" + trainingDatasetType +
      '}';
  }
}
