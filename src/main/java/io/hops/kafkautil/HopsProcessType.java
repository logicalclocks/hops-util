package io.hops.kafkautil;

import java.io.Serializable;

/**
 * Defines the process type, either Producer or Consumer.
 * <p>
 */
public enum HopsProcessType implements Serializable{
  PRODUCER,
  CONSUMER;
}
