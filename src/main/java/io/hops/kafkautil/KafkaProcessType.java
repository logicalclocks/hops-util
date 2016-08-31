package io.hops.kafkautil;

import java.io.Serializable;

/**
 * Defines the process type, either Producer or Consumer.
 * <p>
 */
public enum KafkaProcessType implements Serializable{
  PRODUCER,
  CONSUMER;
}
