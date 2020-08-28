package io.hops.util.featurestore.ops.write_ops;

import io.hops.util.FeaturestoreRestClient;
import io.hops.util.Hops;
import io.hops.util.exceptions.FeaturegroupCreationError;
import io.hops.util.exceptions.FeaturestoreNotFound;
import io.hops.util.exceptions.JWTNotFoundException;
import io.hops.util.featurestore.FeaturestoreHelper;
import io.hops.util.featurestore.dtos.app.FeaturestoreMetadataDTO;
import io.hops.util.featurestore.dtos.featuregroup.CachedFeaturegroupDTO;
import io.hops.util.featurestore.dtos.featuregroup.FeaturegroupDTO;
import io.hops.util.featurestore.dtos.jobs.FeaturestoreJobDTO;
import io.hops.util.featurestore.ops.FeaturestoreOp;

import javax.xml.bind.JAXBException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builder class for Sync-HiveTable operation on the Hopsworks Featurestore.
 */
public class FeaturestoreSyncHiveTable extends FeaturestoreOp {
  
  /**
   * Constructor
   *
   * @param name name of the hive table to synchronize with the feature store
   */
  public FeaturestoreSyncHiveTable(String name) {
    super(name);
  }
  
  /**
   * Method call to execute read operation
   */
  public Object read() {
    throw new UnsupportedOperationException("read() is not supported on a write operation");
  }
  
  /**
   * Synchronizes the hive table with the feature store
   *
   * @throws JAXBException JAXBException
   * @throws FeaturestoreNotFound FeaturestoreNotFound
   */
  public void write()
    throws JAXBException, FeaturestoreNotFound, FeaturegroupCreationError, JWTNotFoundException {
    featurestore = FeaturestoreHelper.featurestoreGetOrDefault(featurestore);
    if(onDemand) {
      throw new IllegalArgumentException("Cannot Synchronize a Hive Table as an on-demand feature group, only as a " +
        "cached feature group");
    }
    FeaturestoreMetadataDTO featurestoreMetadata = FeaturestoreHelper.getFeaturestoreMetadataCache();
    FeaturestoreRestClient.syncHiveTableWithFeaturestoreRest(groupInputParamsIntoDTO(),
      FeaturestoreHelper.getFeaturegroupDtoTypeStr(featurestoreMetadata.getSettings(), false));
    
    //Update metadata cache since we created a new feature group
    Hops.updateFeaturestoreMetadataCache().setFeaturestore(featurestore).write();
  }
  
  /**
   * Group input parameters into a DTO for creating a cached feature group
   *
   * @return DTO representation of the input parameters
   */
  private FeaturegroupDTO groupInputParamsIntoDTO(){
    if(FeaturestoreHelper.jobNameGetOrDefault(null) != null){
      jobs.add(FeaturestoreHelper.jobNameGetOrDefault(null));
    }
    List<FeaturestoreJobDTO> jobsDTOs = jobs.stream().map(jobName -> {
      FeaturestoreJobDTO featurestoreJobDTO = new FeaturestoreJobDTO();
      featurestoreJobDTO.setJobName(jobName);
      return featurestoreJobDTO;
    }).collect(Collectors.toList());
    FeaturegroupDTO featuregroupDTO = new CachedFeaturegroupDTO();
    featuregroupDTO.setFeaturestoreName(featurestore);
    featuregroupDTO.setName(name);
    featuregroupDTO.setVersion(version);
    featuregroupDTO.setDescription(description);
    featuregroupDTO.setJobs(jobsDTOs);
    return featuregroupDTO;
  }
  
  public FeaturestoreSyncHiveTable setFeaturestore(String featurestore) {
    this.featurestore = featurestore;
    return this;
  }
  
  public FeaturestoreSyncHiveTable setName(String name) {
    this.name = name;
    return this;
  }
  
  public FeaturestoreSyncHiveTable setVersion(int version) {
    this.version = version;
    return this;
  }
  
  public FeaturestoreSyncHiveTable setJobs(List<String> jobs) {
    this.jobs = jobs;
    return this;
  }
  
  public FeaturestoreSyncHiveTable setDescription(String description) {
    this.description = description;
    return this;
  }
}
