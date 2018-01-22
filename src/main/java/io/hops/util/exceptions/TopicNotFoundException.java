package io.hops.util.exceptions;

/**
 * Exception thrown when the topic could not be retrieved from Hopsworks.
 * 
 */
public class TopicNotFoundException extends Exception {

  public TopicNotFoundException(String message) {
    super(message);
  }

}
