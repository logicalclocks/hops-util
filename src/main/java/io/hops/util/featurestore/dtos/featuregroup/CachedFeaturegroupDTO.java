/*
 * This file is part of Hopsworks
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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

package io.hops.util.featurestore.dtos.featuregroup;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.hops.util.featurestore.dtos.feature.FeatureDTO;
import io.hops.util.featurestore.dtos.jobs.FeaturestoreJobDTO;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;
import java.util.List;

/**
 * DTO containing the human-readable information of a cached featrue group in the Hopsworks feature store,
 * can be converted to JSON or XML representation using jaxb.
 */
@XmlRootElement
@JsonIgnoreProperties(ignoreUnknown = true)
public class CachedFeaturegroupDTO extends FeaturegroupDTO {

  private Boolean onlineEnabled = false;
  private Boolean hudiEnabled = false;

  public CachedFeaturegroupDTO() {
    super();
  }
  
  public CachedFeaturegroupDTO(Integer featurestoreId, String featurestoreName, String description,
    Date created, String creator, Integer version, String name,
    Integer id, List<FeatureDTO> features, String location,
    List<FeaturestoreJobDTO> jobs,
    Boolean onlineEnabled, Boolean hudiEnabled, Boolean descStatsEnabled,
    Boolean featCorrEnabled, Boolean featHistEnabled, List<String> statColumns) {
    super(featurestoreId, featurestoreName, description, created, creator, version, name, id, features, location, jobs,
      descStatsEnabled, featCorrEnabled, featHistEnabled, statColumns);
    this.onlineEnabled = onlineEnabled;
    this.hudiEnabled = hudiEnabled;
  }

  public Boolean getOnlineEnabled() {
    return onlineEnabled;
  }

  public void setOnlineEnabled(Boolean onlineEnabled) {
    this.onlineEnabled = onlineEnabled;
  }

  public Boolean getHudiEnabled() {
    return hudiEnabled;
  }

  public void setHudiEnabled(Boolean hudiEnabled) {
    this.hudiEnabled = hudiEnabled;
  }

  @Override
  public String toString() {
    return "CachedFeaturegroupDTO{" +
        "onlineEnabled=" + onlineEnabled +
        ", hudiEnabled=" + hudiEnabled +
        '}';
  }
}
