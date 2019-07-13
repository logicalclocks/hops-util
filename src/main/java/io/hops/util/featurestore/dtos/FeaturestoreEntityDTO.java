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

package io.hops.util.featurestore.dtos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.hops.util.featurestore.dtos.feature.FeatureDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.dtos.stats.cluster_analysis.ClusterAnalysisDTO;
import io.hops.util.featurestore.dtos.stats.desc_stats.DescriptiveStatsDTO;
import io.hops.util.featurestore.dtos.stats.feature_correlation.FeatureCorrelationMatrixDTO;
import io.hops.util.featurestore.dtos.stats.feature_distributions.FeatureDistributionsDTO;
import io.hops.util.featurestore.dtos.trainingdataset.TrainingDatasetDTO;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;
import java.util.Date;
import java.util.List;

/**
 * Abstract storage entity in the featurestore. Contains the common fields and functionality between feature groups
 * and training dataset entities.
 */
@XmlRootElement
@XmlSeeAlso({FeaturegroupDTO.class, TrainingDatasetDTO.class})
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
    @JsonSubTypes.Type(value = FeaturegroupDTO.class, name = "FeaturegroupDTO"),
    @JsonSubTypes.Type(value = TrainingDatasetDTO.class, name = "TrainingDatasetDTO")})
public abstract class FeaturestoreEntityDTO {
  private Integer featurestoreId;
  private String featurestoreName;
  private String description;
  private Date created;
  private String creator;
  private Integer jobId;
  private String jobName;
  private Date lastComputed;
  private String jobStatus;
  private Integer version;
  private DescriptiveStatsDTO descriptiveStatistics;
  private FeatureCorrelationMatrixDTO featureCorrelationMatrix;
  private FeatureDistributionsDTO featuresHistogram;
  private ClusterAnalysisDTO clusterAnalysis;
  private String name;
  private Integer id;
  private List<FeatureDTO> features;
  private String location;

  public FeaturestoreEntityDTO() {
  }

  public FeaturestoreEntityDTO(Integer featurestoreId, String featurestoreName, String description, Date created,
                               String creator, Integer jobId, String jobName, Date lastComputed,
                               String jobStatus, Integer version, DescriptiveStatsDTO descriptiveStatistics,
                               FeatureCorrelationMatrixDTO featureCorrelationMatrix,
                               FeatureDistributionsDTO featuresHistogram,
                               ClusterAnalysisDTO clusterAnalysis, String name, Integer id,
                               List<FeatureDTO> features, String location) {
    this.featurestoreId = featurestoreId;
    this.featurestoreName = featurestoreName;
    this.description = description;
    this.created = created;
    this.creator = creator;
    this.jobId = jobId;
    this.jobName = jobName;
    this.lastComputed = lastComputed;
    this.jobStatus = jobStatus;
    this.version = version;
    this.descriptiveStatistics = descriptiveStatistics;
    this.featureCorrelationMatrix = featureCorrelationMatrix;
    this.featuresHistogram = featuresHistogram;
    this.clusterAnalysis = clusterAnalysis;
    this.name = name;
    this.id = id;
    this.features = features;
    this.location = location;
  }

  @XmlElement
  public Date getCreated() {
    return created;
  }

  @XmlElement
  public String getCreator() {
    return creator;
  }

  @XmlElement(nillable = true)
  public Integer getJobId() {
    return jobId;
  }

  @XmlElement(nillable = true)
  public String getJobName() {
    return jobName;
  }

  @XmlElement(nillable = true)
  public Date getLastComputed() {
    return lastComputed;
  }

  @XmlElement
  public String getDescription() {
    return description;
  }

  @XmlElement
  public Integer getVersion() {
    return version;
  }

  @XmlElement(nillable = true)
  public String getJobStatus() {
    return jobStatus;
  }

  @XmlElement
  public Integer getFeaturestoreId() {
    return featurestoreId;
  }

  @XmlElement
  public String getFeaturestoreName() {
    return featurestoreName;
  }

  @XmlElement(nillable = true)
  public FeatureCorrelationMatrixDTO getFeatureCorrelationMatrix() {
    return featureCorrelationMatrix;
  }

  @XmlElement(nillable = true)
  public FeatureDistributionsDTO getFeaturesHistogram() {
    return featuresHistogram;
  }

  @XmlElement(nillable = true)
  public ClusterAnalysisDTO getClusterAnalysis() {
    return clusterAnalysis;
  }

  @XmlElement(nillable = true)
  public DescriptiveStatsDTO getDescriptiveStatistics() {
    return descriptiveStatistics;
  }

  @XmlElement
  public String getName() {
    return name;
  }

  @XmlElement
  public Integer getId() {
    return id;
  }

  @XmlElement
  public List<FeatureDTO> getFeatures() {
    return features;
  }
  
  @XmlElement
  public String getLocation() {
    return location;
  }
  
  public void setLocation(String location) {
    this.location = location;
  }
  
  public void setFeatures(List<FeatureDTO> features) {
    this.features = features;
  }

  public void setFeaturestoreName(String featurestoreName) {
    this.featurestoreName = featurestoreName;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public void setName(String name) {
    this.name = name;
  }
  
  public void setDescriptiveStatistics(
    DescriptiveStatsDTO descriptiveStatistics) {
    this.descriptiveStatistics = descriptiveStatistics;
  }
  
  public void setFeatureCorrelationMatrix(
    FeatureCorrelationMatrixDTO featureCorrelationMatrix) {
    this.featureCorrelationMatrix = featureCorrelationMatrix;
  }
  
  public void setFeaturesHistogram(
    FeatureDistributionsDTO featuresHistogram) {
    this.featuresHistogram = featuresHistogram;
  }
  
  public void setClusterAnalysis(
    ClusterAnalysisDTO clusterAnalysis) {
    this.clusterAnalysis = clusterAnalysis;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public void setFeaturestoreId(Integer featurestoreId) {
    this.featurestoreId = featurestoreId;
  }

  public void setCreated(Date created) {
    this.created = created;
  }

  public void setCreator(String creator) {
    this.creator = creator;
  }

  public void setJobId(Integer jobId) {
    this.jobId = jobId;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public void setLastComputed(Date lastComputed) {
    this.lastComputed = lastComputed;
  }

  public void setJobStatus(String jobStatus) {
    this.jobStatus = jobStatus;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  @Override
  public String toString() {
    return "FeaturestoreEntityDTO{" +
        "featurestoreId=" + featurestoreId +
        ", featurestoreName='" + featurestoreName + '\'' +
        ", description='" + description + '\'' +
        ", created='" + created + '\'' +
        ", creator='" + creator + '\'' +
        ", jobId=" + jobId +
        ", jobName='" + jobName + '\'' +
        ", lastComputed='" + lastComputed + '\'' +
        ", jobStatus='" + jobStatus + '\'' +
        ", version=" + version +
        ", descriptiveStatistics=" + descriptiveStatistics +
        ", featureCorrelationMatrix=" + featureCorrelationMatrix +
        ", featuresHistogram=" + featuresHistogram +
        ", clusterAnalysis=" + clusterAnalysis +
        ", name='" + name + '\'' +
        ", id=" + id +
        '}';
  }
}
