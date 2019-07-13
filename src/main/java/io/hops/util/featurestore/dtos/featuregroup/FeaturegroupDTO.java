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

package io.hops.util.featurestore.dtos.featuregroup;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.hops.util.featurestore.dtos.FeaturestoreEntityDTO;
import io.hops.util.featurestore.dtos.feature.FeatureDTO;
import io.hops.util.featurestore.dtos.stats.cluster_analysis.ClusterAnalysisDTO;
import io.hops.util.featurestore.dtos.stats.desc_stats.DescriptiveStatsDTO;
import io.hops.util.featurestore.dtos.stats.feature_correlation.FeatureCorrelationMatrixDTO;
import io.hops.util.featurestore.dtos.stats.feature_distributions.FeatureDistributionsDTO;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import java.util.Date;
import java.util.List;

/**
 * DTO containing the human-readable information of a featuregroup, can be converted to JSON or XML representation
 * using jaxb.
 */
@XmlRootElement
@XmlSeeAlso({CachedFeaturegroupDTO.class, OnDemandFeaturegroupDTO.class})
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
    @JsonSubTypes.Type(value = CachedFeaturegroupDTO.class, name = "CachedFeaturegroupDTO"),
    @JsonSubTypes.Type(value = OnDemandFeaturegroupDTO.class, name = "HopsfsTrainingDatasetDTO")})
public class FeaturegroupDTO extends FeaturestoreEntityDTO {

  private FeaturegroupType featuregroupType;

  public FeaturegroupDTO() {
  }

  public FeaturegroupDTO(Integer featurestoreId, String featurestoreName,
                         String description, Date created, String creator,
                         Integer jobId, String jobName, Date lastComputed, String jobStatus,
                         Integer version, DescriptiveStatsDTO descriptiveStatistics,
                         FeatureCorrelationMatrixDTO featureCorrelationMatrix,
                         FeatureDistributionsDTO featuresHistogram, ClusterAnalysisDTO clusterAnalysis,
                         String name, Integer id, List<FeatureDTO> features, String location,
                         FeaturegroupType featuregroupType) {
    super(featurestoreId, featurestoreName, description, created, creator, jobId, jobName, lastComputed,
        jobStatus, version, descriptiveStatistics, featureCorrelationMatrix, featuresHistogram, clusterAnalysis,
        name, id, features, location);
    this.featuregroupType = featuregroupType;
  }

  @XmlElement
  public FeaturegroupType getFeaturegroupType() {
    return featuregroupType;
  }
  
  public void setFeaturegroupType(FeaturegroupType featuregroupType) {
    this.featuregroupType = featuregroupType;
  }

  @Override
  public String toString() {
    return "FeaturegroupDTO{" +
        "featuregroupType=" + featuregroupType +
        '}';
  }


}
