/*
 * Changes to this file committed after and not including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
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

package io.hops.util.featurestore;

import io.hops.util.featurestore.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.trainingdataset.TrainingDatasetDTO;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.List;

/**
 * DTO containing the human-readable information of featuregroups and training datasets in a featurestore,
 * can be converted to JSON or XML representation using jaxb.
 */
@XmlRootElement
@XmlType(propOrder = {"featuregroups", "trainingDatasets"})
public class FeaturegroupsAndTrainingDatasetsDTO {

  private List<FeaturegroupDTO> featuregroups;
  private List<TrainingDatasetDTO> trainingDatasets;

  public FeaturegroupsAndTrainingDatasetsDTO() {
  }

  public FeaturegroupsAndTrainingDatasetsDTO(
      List<FeaturegroupDTO> featuregroups, List<TrainingDatasetDTO> trainingDatasets) {
    this.featuregroups = featuregroups;
    this.trainingDatasets = trainingDatasets;
  }

  @XmlElement
  public List<FeaturegroupDTO> getFeaturegroups() {
    return featuregroups;
  }

  @XmlElement
  public List<TrainingDatasetDTO> getTrainingDatasets() {
    return trainingDatasets;
  }

  public void setFeaturegroups(List<FeaturegroupDTO> featuregroups) {
    this.featuregroups = featuregroups;
  }

  public void setTrainingDatasets(List<TrainingDatasetDTO> trainingDatasets) {
    this.trainingDatasets = trainingDatasets;
  }

  @Override
  public String toString() {
    return "FeaturegroupsAndTrainingDatasetsDTO{" +
        "featuregroups=" + featuregroups +
        ", trainingDatasets=" + trainingDatasets +
        '}';
  }
}
