package io.hops.util;

/**
 * Exception thrown when the topic could not be retrieved from Hopsworks.
 * <p>
 */
public class TopicNotFoundException extends Exception {

  public TopicNotFoundException(String message) {
    super(message);
  }

}
