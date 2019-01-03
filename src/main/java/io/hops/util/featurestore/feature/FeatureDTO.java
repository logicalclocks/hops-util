/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.hops.util.featurestore.feature;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * FeatureDTO POJO parsed from JSON response
 */
@XmlRootElement
public class FeatureDTO {

  private String name;
  private String type;
  private String description;
  private Boolean primary = false;

  public FeatureDTO() {
  }

  public FeatureDTO(String name, String type, String description, Boolean primary) {
    this.name = name;
    this.type = type;
    this.description = description;
    this.primary = primary;
  }

  @XmlElement
  public String getName() {
    return name;
  }

  @XmlElement
  public String getType() {
    return type;
  }

  @XmlElement
  public String getDescription() {
    return description;
  }

  @XmlElement
  public Boolean getPrimary() {
    return primary;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public void setPrimary(Boolean primary) {
    this.primary = primary;
  }

  @Override
  public String toString() {
    return "FeatureDTO{" +
        "name='" + name + '\'' +
        ", type='" + type + '\'' +
        ", description='" + description + '\'' +
        ", primary=" + primary +
        '}';
  }
}
