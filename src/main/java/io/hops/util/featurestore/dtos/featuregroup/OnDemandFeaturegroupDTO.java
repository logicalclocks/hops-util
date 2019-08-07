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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * DTO containing the human-readable information of an on-demand featuregroup in the feature store, can be
 * converted to JSON or XML representation using jaxb.
 */
@XmlRootElement
public class OnDemandFeaturegroupDTO extends FeaturegroupDTO {
  
  private Integer jdbcConnectorId;
  private String jdbcConnectorName;
  private String query;
  
  public OnDemandFeaturegroupDTO() {
    super();
  }

  public OnDemandFeaturegroupDTO(Integer jdbcConnectorId, String jdbcConnectorName, String query) {
    this.jdbcConnectorId = jdbcConnectorId;
    this.jdbcConnectorName = jdbcConnectorName;
    this.query = query;
  }

  @XmlElement
  public Integer getJdbcConnectorId() {
    return jdbcConnectorId;
  }
  
  public void setJdbcConnectorId(Integer jdbcConnectorId) {
    this.jdbcConnectorId = jdbcConnectorId;
  }
  
  @XmlElement
  public String getQuery() {
    return query;
  }
  
  public void setQuery(String query) {
    this.query = query;
  }
  
  @XmlElement
  public String getJdbcConnectorName() {
    return jdbcConnectorName;
  }
  
  public void setJdbcConnectorName(String jdbcConnectorName) {
    this.jdbcConnectorName = jdbcConnectorName;
  }

  @Override
  public String toString() {
    return "HopsfsTrainingDatasetDTO{" +
        "jdbcConnectorId=" + jdbcConnectorId +
        ", jdbcConnectorName='" + jdbcConnectorName + '\'' +
        ", query='" + query + '\'' +
        '}';
  }
}
