package io.hops.util.spark;

import io.hops.util.CredentialsNotFoundException;
import io.hops.util.HopsProducer;
import io.hops.util.SchemaNotFoundException;
import java.util.Properties;

/**
 * Hops wrapper for a Spark producer.
 * 
 */
public class SparkProducer extends HopsProducer {

  public SparkProducer(String topic, Properties userProps) throws SchemaNotFoundException,
      CredentialsNotFoundException {
    super(topic, userProps);
  }
}
