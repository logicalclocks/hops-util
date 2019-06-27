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

package io.hops.util.featurestore.dtos;

import io.hops.util.featurestore.dtos.stats.cluster_analysis.ClusterAnalysisDTO;
import io.hops.util.featurestore.dtos.stats.desc_stats.DescriptiveStatsDTO;
import io.hops.util.featurestore.dtos.stats.feature_correlation.FeatureCorrelationMatrixDTO;
import io.hops.util.featurestore.dtos.stats.feature_distributions.FeatureDistributionsDTO;

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
  private String inputFormat;
  private HiveTableType hiveTableType;
  
  public FeaturegroupDTO(Integer featurestoreId, String featurestoreName, String description, Date created,
    String creator, Integer jobId, String jobName, Date lastComputed, String jobStatus, Integer version,
    Long inodeId, DescriptiveStatsDTO descriptiveStatistics,
    FeatureCorrelationMatrixDTO featureCorrelationMatrix,
    FeatureDistributionsDTO featuresHistogram,
    ClusterAnalysisDTO clusterAnalysis, String name, Integer id,
    List<FeatureDTO> features, String location, List<String> hdfsStorePaths, String inputFormat,
    HiveTableType hiveTableType) {
    super(featurestoreId, featurestoreName, description, created, creator, jobId, jobName, lastComputed, jobStatus,
      version, inodeId, descriptiveStatistics, featureCorrelationMatrix, featuresHistogram, clusterAnalysis, name, id,
      features, location);
    this.hdfsStorePaths = hdfsStorePaths;
    this.inputFormat = inputFormat;
    this.hiveTableType = hiveTableType;
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
  
  @XmlElement
  public String getInputFormat() {
    return inputFormat;
  }
  
  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }
  
  @XmlElement
  public HiveTableType getHiveTableType() {
    return hiveTableType;
  }
  
  public void setHiveTableType(HiveTableType hiveTableType) {
    this.hiveTableType = hiveTableType;
  }
  
  @Override
  public String toString() {
    return "FeaturegroupDTO{" +
      "hdfsStorePaths=" + hdfsStorePaths +
      ", inputFormat='" + inputFormat + '\'' +
      ", hiveTableType=" + hiveTableType +
      '}';
  }
}
