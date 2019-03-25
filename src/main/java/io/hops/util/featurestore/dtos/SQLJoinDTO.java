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

package io.hops.util.featurestore.dtos;

import java.util.List;

/**
 * SQL Join DTO, when JOIN str is constructed we also assign alias to each featuregroup
 */
public class SQLJoinDTO {
  String joinStr;
  List<FeaturegroupDTO> featuregroupDTOS;

  public SQLJoinDTO(String joinStr, List<FeaturegroupDTO> featuregroupDTOS) {
    this.joinStr = joinStr;
    this.featuregroupDTOS = featuregroupDTOS;
  }

  public String getJoinStr() {
    return joinStr;
  }

  public List<FeaturegroupDTO> getFeaturegroupDTOS() {
    return featuregroupDTOS;
  }
}
