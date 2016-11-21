package io.hops.hopsutil.spark;

import io.hops.hopsutil.HopsProducer;
import io.hops.hopsutil.SchemaNotFoundException;

/**
 *
 * @author teo
 */
public class SparkProducer extends HopsProducer {

  public SparkProducer(String topic) throws SchemaNotFoundException {
    super(topic);
  }
}
