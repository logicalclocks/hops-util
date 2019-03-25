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

package io.hops.util.featurestore.dtos.stats.feature_correlation;

import io.hops.util.featurestore.dtos.stats.FeaturestoreStatisticType;
import io.hops.util.featurestore.dtos.stats.FeaturestoreStatisticValue;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * XML/JSON DTO representing a correlation value for a feature
 */
@XmlRootElement
@XmlType(propOrder = {"featureName", "correlation", "statisticType"})
public class CorrelationValueDTO extends FeaturestoreStatisticValue {

  private String featureName;
  private Float correlation;

  public CorrelationValueDTO() {
  }

  @XmlElement
  public String getFeatureName() {
    return featureName;
  }

  @XmlElement
  public Float getCorrelation() {
    return correlation;
  }

  public void setFeatureName(String featureName) {
    this.featureName = featureName;
  }

  public void setCorrelation(Float correlation) {
    this.correlation = correlation;
  }

  @Override
  @XmlElement(name="statisticType")
  public FeaturestoreStatisticType getStatisticType() {
    return FeaturestoreStatisticType.FEATURECORRELATIONS;
  }

  @Override
  public String toString() {
    return "CorrelationValueDTO{" +
        "featureName='" + featureName + '\'' +
        ", correlation=" + correlation +
        '}';
  }
}
