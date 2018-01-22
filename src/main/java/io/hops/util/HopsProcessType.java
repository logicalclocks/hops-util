package io.hops.util;

import java.io.Serializable;

/**
 * Defines the process type, either Producer or Consumer.
 * 
 */
public enum HopsProcessType implements Serializable{
  PRODUCER,
  CONSUMER;
}
