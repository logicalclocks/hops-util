package io.hops.util.exceptions;

/**
 * Exception thrown when the schema for the topic cannot be retrieved.
 * 
 */
public class SchemaNotFoundException extends Exception {

  Integer status;

  public SchemaNotFoundException(String message) {
    super(message);
  }

  public SchemaNotFoundException(Integer status, String message) {
    super(message);
    this.status = status;
  }

  public Integer getStatus() {
    return status;
  }

  public void setStatus(Integer status) {
    this.status = status;
  }
}
