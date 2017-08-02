package io.hops.util.spark;

import io.hops.util.CredentialsNotFoundException;
import io.hops.util.HopsProducer;
import io.hops.util.SchemaNotFoundException;

/**
 *
 * @author teo
 */
public class SparkProducer extends HopsProducer {

  public SparkProducer(String topic) throws SchemaNotFoundException, CredentialsNotFoundException {
    super(topic);
  }
}
