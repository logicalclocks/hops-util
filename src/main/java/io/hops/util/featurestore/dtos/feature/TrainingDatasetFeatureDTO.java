package io.hops.util.featurestore.dtos.feature;

public class TrainingDatasetFeatureDTO {
  private String name;
  private String type;
  private Integer index;

  public TrainingDatasetFeatureDTO(String name, String type, Integer index) {
    this.name = name;
    this.type = type;
    this.index = index;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public Integer getIndex() {
    return index;
  }

  public void setIndex(Integer index) {
    this.index = index;
  }
}
