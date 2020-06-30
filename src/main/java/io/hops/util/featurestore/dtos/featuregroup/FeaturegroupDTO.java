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
import io.hops.util.featurestore.dtos.jobs.FeaturestoreJobDTO;
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

  @XmlElement
  private Boolean descStatsEnabled;
  @XmlElement
  private Boolean featCorrEnabled;
  @XmlElement
  private Boolean featHistEnabled;
  @XmlElement
  private Boolean clusterAnalysisEnabled;
  @XmlElement
  private List<String> statisticColumns;
  @XmlElement
  private Integer numBins;
  @XmlElement
  private Integer numClusters;
  @XmlElement
  private String corrMethod;

  public FeaturegroupDTO() {
  }
  
  public FeaturegroupDTO(Integer featurestoreId, String featurestoreName, String description, Date created,
    String creator, Integer version,
    DescriptiveStatsDTO descriptiveStatistics,
    FeatureCorrelationMatrixDTO featureCorrelationMatrix,
    FeatureDistributionsDTO featuresHistogram,
    ClusterAnalysisDTO clusterAnalysis, String name,
    Integer id, List<FeatureDTO> features, String location,
    List<FeaturestoreJobDTO> jobs,
    Boolean clusterAnalysisEnabled, Boolean descStatsEnabled, Boolean featCorrEnabled, Boolean featHistEnabled,
    List<String> statColumns, Integer numClusters, Integer numBins, String corrMethod) {
    super(featurestoreId, featurestoreName, description, created, creator, version, descriptiveStatistics,
      featureCorrelationMatrix, featuresHistogram, clusterAnalysis, name, id, features, location, jobs);
    this.clusterAnalysisEnabled = clusterAnalysisEnabled;
    this.descStatsEnabled = descStatsEnabled;
    this.featCorrEnabled = featCorrEnabled;
    this.featHistEnabled = featHistEnabled;
    this.statisticColumns = statColumns;
    this.numBins = numBins;
    this.numClusters = numClusters;
    this.corrMethod = corrMethod;
  }
  
  public Boolean isDescStatsEnabled() {
    return descStatsEnabled;
  }
  
  public void setDescStatsEnabled(boolean descStatsEnabled) {
    this.descStatsEnabled = descStatsEnabled;
  }
  
  public Boolean isFeatCorrEnabled() {
    return featCorrEnabled;
  }
  
  public void setFeatCorrEnabled(boolean featCorrEnabled) {
    this.featCorrEnabled = featCorrEnabled;
  }
  
  public Boolean isFeatHistEnabled() {
    return featHistEnabled;
  }
  
  public void setFeatHistEnabled(boolean featHistEnabled) {
    this.featHistEnabled = featHistEnabled;
  }
  
  public Boolean isClusterAnalysisEnabled() {
    return clusterAnalysisEnabled;
  }
  
  public void setClusterAnalysisEnabled(boolean clusterAnalysisEnabled) {
    this.clusterAnalysisEnabled = clusterAnalysisEnabled;
  }
  
  public List<String> getStatisticColumns() {
    return statisticColumns;
  }
  
  public void setStatisticColumns(List<String> statisticColumns) {
    this.statisticColumns = statisticColumns;
  }
  
  public Integer getNumBins() {
    return numBins;
  }
  
  public void setNumBins(Integer numBins) {
    this.numBins = numBins;
  }
  
  public Integer getNumClusters() {
    return numClusters;
  }
  
  public void setNumClusters(Integer numClusters) {
    this.numClusters = numClusters;
  }
  
  public String getCorrMethod() {
    return corrMethod;
  }
  
  public void setCorrMethod(String corrMethod) {
    this.corrMethod = corrMethod;
  }

  @Override
  public String toString() {
    return "FeaturegroupDTO{" +
      "descStatsEnabled=" + descStatsEnabled +
      ", featCorrEnabled=" + featCorrEnabled +
      ", featHistEnabled=" + featHistEnabled +
      ", clusterAnalysisEnabled=" + clusterAnalysisEnabled +
      ", statisticColumns=" + statisticColumns +
      ", numBins=" + numBins +
      ", numClusters=" + numClusters +
      ", corrMethod='" + corrMethod + '\'' +
      '}';
  }

}
