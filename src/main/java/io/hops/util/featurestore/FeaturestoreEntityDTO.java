package io.hops.util.featurestore;


import io.hops.util.featurestore.dependencies.FeaturestoreDependencyDTO;
import io.hops.util.featurestore.feature.FeatureDTO;
import io.hops.util.featurestore.stats.cluster_analysis.ClusterAnalysisDTO;
import io.hops.util.featurestore.stats.desc_stats.DescriptiveStatsDTO;
import io.hops.util.featurestore.stats.feature_correlation.FeatureCorrelationMatrixDTO;
import io.hops.util.featurestore.stats.feature_distributions.FeatureDistributionsDTO;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;
import java.util.List;

/**
 * Abstract storage entity in the featurestore. Contains the common fields and functionality between feature groups
 * and training dataset entities.
 */
@XmlRootElement
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
  private Long inodeId;
  private DescriptiveStatsDTO descriptiveStatistics;
  private FeatureCorrelationMatrixDTO featureCorrelationMatrix;
  private FeatureDistributionsDTO featuresHistogram;
  private ClusterAnalysisDTO clusterAnalysis;
  private String name;
  private Integer id;
  private List<FeaturestoreDependencyDTO> dependencies;
  private List<FeatureDTO> features;

  public FeaturestoreEntityDTO(
      Integer featurestoreId, String featurestoreName, String description, Date created, String creator,
      Integer jobId, String jobName, Date lastComputed, String jobStatus, Integer version, Long inodeId,
      DescriptiveStatsDTO descriptiveStatistics, FeatureCorrelationMatrixDTO featureCorrelationMatrix,
      FeatureDistributionsDTO featuresHistogram, ClusterAnalysisDTO clusterAnalysis, String name,
      Integer id, List<FeaturestoreDependencyDTO> dependencies, List<FeatureDTO> features) {
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
    this.inodeId = inodeId;
    this.descriptiveStatistics = descriptiveStatistics;
    this.featureCorrelationMatrix = featureCorrelationMatrix;
    this.featuresHistogram = featuresHistogram;
    this.clusterAnalysis = clusterAnalysis;
    this.name = name;
    this.id = id;
    this.dependencies = dependencies;
    this.features = features;
  }

  public FeaturestoreEntityDTO() {
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
  public Long getInodeId() {
    return inodeId;
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
  public List<FeaturestoreDependencyDTO> getDependencies() {
    return dependencies;
  }

  @XmlElement
  public List<FeatureDTO> getFeatures() {
    return features;
  }

  public void setFeaturestoreId(Integer featurestoreId) {
    this.featurestoreId = featurestoreId;
  }

  public void setFeaturestoreName(String featurestoreName) {
    this.featurestoreName = featurestoreName;
  }

  public void setDescription(String description) {
    this.description = description;
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

  public void setInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }

  public void setDescriptiveStatistics(DescriptiveStatsDTO descriptiveStatistics) {
    this.descriptiveStatistics = descriptiveStatistics;
  }

  public void setFeatureCorrelationMatrix(FeatureCorrelationMatrixDTO featureCorrelationMatrix) {
    this.featureCorrelationMatrix = featureCorrelationMatrix;
  }

  public void setFeaturesHistogram(FeatureDistributionsDTO featuresHistogram) {
    this.featuresHistogram = featuresHistogram;
  }

  public void setClusterAnalysis(ClusterAnalysisDTO clusterAnalysis) {
    this.clusterAnalysis = clusterAnalysis;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public void setDependencies(List<FeaturestoreDependencyDTO> dependencies) {
    this.dependencies = dependencies;
  }

  public void setFeatures(List<FeatureDTO> features) {
    this.features = features;
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
        ", inodeId=" + inodeId +
        ", descriptiveStatistics=" + descriptiveStatistics +
        ", featureCorrelationMatrix=" + featureCorrelationMatrix +
        ", featuresHistogram=" + featuresHistogram +
        ", clusterAnalysis=" + clusterAnalysis +
        ", dependencies=" + dependencies +
        ", name='" + name + '\'' +
        ", id=" + id +
        '}';
  }
}
