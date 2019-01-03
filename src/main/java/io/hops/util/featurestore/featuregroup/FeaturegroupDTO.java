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

package io.hops.util.featurestore.featuregroup;

import io.hops.util.featurestore.FeaturestoreEntityDTO;
import io.hops.util.featurestore.dependencies.FeaturestoreDependencyDTO;
import io.hops.util.featurestore.feature.FeatureDTO;
import io.hops.util.featurestore.stats.cluster_analysis.ClusterAnalysisDTO;
import io.hops.util.featurestore.stats.desc_stats.DescriptiveStatsDTO;
import io.hops.util.featurestore.stats.feature_correlation.FeatureCorrelationMatrixDTO;
import io.hops.util.featurestore.stats.feature_distributions.FeatureDistributionsDTO;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;
import java.util.List;

/**
 * DTO containing the human-readable information of a featuregroup, can be converted to JSON or XML representation
 * using jaxb.
 */
@XmlRootElement
public class FeaturegroupDTO extends FeaturestoreEntityDTO {

  private List<String> hdfsStorePaths;

  public FeaturegroupDTO(
      Integer featurestoreId, String featurestoreName, String description, Date created,
      String creator, Integer jobId, String jobName, Date lastComputed, String jobStatus, Integer version,
      Long inodeId, DescriptiveStatsDTO descriptiveStatistics, FeatureCorrelationMatrixDTO featureCorrelationMatrix,
      FeatureDistributionsDTO featuresHistogram, ClusterAnalysisDTO clusterAnalysis, String name, Integer id,
      List<FeaturestoreDependencyDTO> dependencies, List<FeatureDTO> features, List<String> hdfsStorePaths) {
    super(featurestoreId, featurestoreName, description, created, creator, jobId, jobName, lastComputed, jobStatus,
        version, inodeId, descriptiveStatistics, featureCorrelationMatrix, featuresHistogram, clusterAnalysis,
        name, id, dependencies, features);
    this.hdfsStorePaths = hdfsStorePaths;
  }

  public FeaturegroupDTO() {
  }

  @XmlElement
  public List<String> getHdfsStorePaths() {
    return hdfsStorePaths;
  }


  public void setHdfsStorePaths(List<String> hdfsStorePaths) {
    this.hdfsStorePaths = hdfsStorePaths;
  }

  @Override
  public String toString() {
    return "FeaturegroupDTO{" +
        ", hdfsStorePaths=" + hdfsStorePaths +
        '}';
  }

}
