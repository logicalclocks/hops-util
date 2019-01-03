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

package io.hops.util.featurestore.stats.cluster_analysis;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * XML/JSON DTO representing an individual datapoint assigned to a cluster
 */
@XmlRootElement
@XmlType(propOrder = {"datapointName", "cluster"})
public class ClusterDTO {

  private String datapointName;
  private Integer cluster;

  public ClusterDTO() {
  }

  public ClusterDTO(String datapointName, Integer cluster) {
    this.datapointName = datapointName;
    this.cluster = cluster;
  }

  @XmlElement
  public String getDatapointName() {
    return datapointName;
  }

  @XmlElement
  public Integer getCluster() {
    return cluster;
  }

  public void setDatapointName(String datapointName) {
    this.datapointName = datapointName;
  }

  public void setCluster(Integer cluster) {
    this.cluster = cluster;
  }

  @Override
  public String toString() {
    return "ClusterDTO{" +
        "datapointName='" + datapointName + '\'' +
        ", cluster=" + cluster +
        '}';
  }
}
