package io.hops.util.exceptions;

/**
 * Thrown when project user credentials (certificates and their password) could not be found in the container.
 * 
 */
public class CredentialsNotFoundException extends Exception {

  Integer status;

  public CredentialsNotFoundException(String message) {
    super(message);
  }

  public CredentialsNotFoundException(Integer status, String message) {
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
