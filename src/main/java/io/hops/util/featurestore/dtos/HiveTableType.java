package io.hops.util.featurestore.dtos;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Type of Hive Table
 */
public enum HiveTableType {
  @JsonProperty("MANAGED_TABLE")
  MANAGED_TABLE,
  @JsonProperty("EXTERNAL_TABLE")
  EXTERNAL_TABLE;
}