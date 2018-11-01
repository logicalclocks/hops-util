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

import io.hops.util.featurestore.stats.FeaturestoreStatisticType;
import io.hops.util.featurestore.stats.FeaturestoreStatisticValue;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.List;

/**
 * XML/JSON DTO representing ClusterAnalysis statistics
 */
@XmlRootElement
@XmlType(propOrder = {"dataPoints", "clusters", "statisticType"})
public class ClusterAnalysisDTO extends FeaturestoreStatisticValue {

  private List<DatapointDTO> dataPoints;
  private List<ClusterDTO> clusters;

  public ClusterAnalysisDTO() {
  }

  @XmlElement
  public List<DatapointDTO> getDataPoints() {
    return dataPoints;
  }

  @XmlElement
  public List<ClusterDTO> getClusters() {
    return clusters;
  }

  public void setDataPoints(List<DatapointDTO> dataPoints) {
    this.dataPoints = dataPoints;
  }

  public void setClusters(List<ClusterDTO> clusters) {
    this.clusters = clusters;
  }

  @Override
  @XmlElement(name="statisticType")
  public FeaturestoreStatisticType getStatisticType() {
    return FeaturestoreStatisticType.CLUSTERANALYSIS;
  }

  @Override
  public String toString() {
    return "ClusterAnalysisDTO{" +
        "dataPoints=" + dataPoints +
        ", clusters=" + clusters +
        '}';
  }
}
