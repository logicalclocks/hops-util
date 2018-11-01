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


package io.hops.util.featurestore.stats;

import io.hops.util.featurestore.stats.cluster_analysis.ClusterAnalysisDTO;
import io.hops.util.featurestore.stats.desc_stats.DescriptiveStatsDTO;
import io.hops.util.featurestore.stats.feature_correlation.FeatureCorrelationMatrixDTO;
import io.hops.util.featurestore.stats.feature_distributions.FeatureDistributionsDTO;

public class StatisticsDTO {

  private DescriptiveStatsDTO descriptiveStatsDTO;
  private ClusterAnalysisDTO clusterAnalysisDTO;
  private FeatureCorrelationMatrixDTO featureCorrelationMatrixDTO;
  private FeatureDistributionsDTO featureDistributionsDTO;

  public StatisticsDTO(
      DescriptiveStatsDTO descriptiveStatsDTO, ClusterAnalysisDTO clusterAnalysisDTO,
      FeatureCorrelationMatrixDTO featureCorrelationMatrixDTO, FeatureDistributionsDTO featureDistributionsDTO) {
    this.descriptiveStatsDTO = descriptiveStatsDTO;
    this.clusterAnalysisDTO = clusterAnalysisDTO;
    this.featureCorrelationMatrixDTO = featureCorrelationMatrixDTO;
    this.featureDistributionsDTO = featureDistributionsDTO;
  }

  public DescriptiveStatsDTO getDescriptiveStatsDTO() {
    return descriptiveStatsDTO;
  }

  public ClusterAnalysisDTO getClusterAnalysisDTO() {
    return clusterAnalysisDTO;
  }

  public FeatureCorrelationMatrixDTO getFeatureCorrelationMatrixDTO() {
    return featureCorrelationMatrixDTO;
  }

  public FeatureDistributionsDTO getFeatureDistributionsDTO() {
    return featureDistributionsDTO;
  }

  public void setDescriptiveStatsDTO(DescriptiveStatsDTO descriptiveStatsDTO) {
    this.descriptiveStatsDTO = descriptiveStatsDTO;
  }

  public void setClusterAnalysisDTO(ClusterAnalysisDTO clusterAnalysisDTO) {
    this.clusterAnalysisDTO = clusterAnalysisDTO;
  }

  public void setFeatureCorrelationMatrixDTO(FeatureCorrelationMatrixDTO featureCorrelationMatrixDTO) {
    this.featureCorrelationMatrixDTO = featureCorrelationMatrixDTO;
  }

  public void setFeatureDistributionsDTO(FeatureDistributionsDTO featureDistributionsDTO) {
    this.featureDistributionsDTO = featureDistributionsDTO;
  }

  @Override
  public String toString() {
    return "StatisticsDTO{" +
        "descriptiveStatsDTO=" + descriptiveStatsDTO +
        ", clusterAnalysisDTO=" + clusterAnalysisDTO +
        ", featureCorrelationMatrixDTO=" + featureCorrelationMatrixDTO +
        ", featureDistributionsDTO=" + featureDistributionsDTO +
        '}';
  }
}
