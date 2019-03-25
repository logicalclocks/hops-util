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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.Date;

/**
 * DTO containing the human-readable information of a featurestore dependency,
 * can be converted to JSON or XML representation using jaxb.
 */
@XmlRootElement
@XmlType(propOrder = {"path", "modification", "inodeId", "dir"})
public class FeaturestoreDependencyDTO {

  private String path;
  private Date modification;
  private Long inodeId;
  private boolean dir;

  public FeaturestoreDependencyDTO(){}

  public FeaturestoreDependencyDTO(String path, Date modification, Long inodeId, boolean dir) {
    this.path = path;
    this.modification = modification;
    this.inodeId = inodeId;
    this.dir = dir;
  }

  @XmlElement
  public String getPath() {
    return path;
  }

  @XmlElement
  public Date getModification() {
    return modification;
  }

  @XmlElement
  public Long getInodeId() {
    return inodeId;
  }

  @XmlElement
  public boolean isDir() {
    return dir;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public void setModification(Date modification) {
    this.modification = modification;
  }

  public void setInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }

  public void setDir(boolean dir) {
    this.dir = dir;
  }

  @Override
  public String toString() {
    return "FeaturestoreDependencyDTO{" +
        ", path='" + path + '\'' +
        ", modification=" + modification +
        ", inodeId=" + inodeId +
        ", dir=" + dir +
        '}';
  }
}
