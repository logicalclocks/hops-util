package io.hops.kafkautil.spark;

import io.hops.kafkautil.HopsProducer;
import io.hops.kafkautil.SchemaNotFoundException;

/**
 *
 * @author teo
 */
public class SparkProducer extends HopsProducer {

  public SparkProducer(String topic) throws SchemaNotFoundException {
    super(topic);
  }
}
