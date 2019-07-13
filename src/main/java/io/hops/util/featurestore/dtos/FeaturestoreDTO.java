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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;

/**
 * DTO containing the human-readable information of a featurestore, can be converted to JSON or XML representation
 * using jaxb.
 */
@XmlRootElement
public class FeaturestoreDTO {

  private Integer featurestoreId;
  private String featurestoreName;
  private Date created;
  private String hdfsStorePath;
  private String projectName;
  private Integer projectId;
  private String featurestoreDescription;
  private Long inodeId;

  public FeaturestoreDTO() {
  }

  public FeaturestoreDTO(Integer featurestoreId, String featurestoreName, Date created, String hdfsStorePath,
                         String projectName, Integer projectId, String featurestoreDescription, Long inodeId) {
    this.featurestoreId = featurestoreId;
    this.featurestoreName = featurestoreName;
    this.created = created;
    this.hdfsStorePath = hdfsStorePath;
    this.projectName = projectName;
    this.projectId = projectId;
    this.featurestoreDescription = featurestoreDescription;
    this.inodeId = inodeId;
  }

  @XmlElement
  public String getHdfsStorePath() {
    return hdfsStorePath;
  }

  @XmlElement
  public String getFeaturestoreName() {
    return featurestoreName;
  }

  @XmlElement
  public Integer getFeaturestoreId() {
    return featurestoreId;
  }

  @XmlElement
  public Date getCreated() {
    return created;
  }

  @XmlElement
  public String getProjectName() {
    return projectName;
  }

  @XmlElement
  public Integer getProjectId() {
    return projectId;
  }

  @XmlElement(nillable = true)
  public String getFeaturestoreDescription() {
    return featurestoreDescription;
  }

  @XmlElement
  public Long getInodeId() {
    return inodeId;
  }

  public void setFeaturestoreDescription(String featurestoreDescription) {
    this.featurestoreDescription = featurestoreDescription;
  }

  public void setFeaturestoreName(String featurestoreName) {
    this.featurestoreName = featurestoreName;
  }

  public void setHdfsStorePath(String hdfsStorePath) {
    this.hdfsStorePath = hdfsStorePath;
  }

  public void setInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }

  public void setFeaturestoreId(Integer featurestoreId) {
    this.featurestoreId = featurestoreId;
  }

  public void setCreated(Date created) {
    this.created = created;
  }

  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }

  public void setProjectId(Integer projectId) {
    this.projectId = projectId;
  }

  @Override
  public String toString() {
    return "FeaturestoreDTO{" +
        "featurestoreId=" + featurestoreId +
        ", featurestoreName='" + featurestoreName + '\'' +
        ", created='" + created + '\'' +
        ", hdfsStorePath='" + hdfsStorePath + '\'' +
        ", projectName='" + projectName + '\'' +
        ", projectId=" + projectId +
        ", featurestoreDescription='" + featurestoreDescription + '\'' +
        ", inodeId=" + inodeId +
        '}';
  }
}
