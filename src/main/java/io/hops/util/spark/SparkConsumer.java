package io.hops.util.spark;

import io.hops.util.HopsUtil;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategy;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import scala.Function0;

/**
 *
 * <p>
 */
public class SparkConsumer {

  private JavaStreamingContext jsc;
  private final Collection<String> topics;
  private final Map<String, Object> kafkaParams;
  private SparkSession sparkSession;

  public SparkConsumer() {
    this.topics = HopsUtil.getTopics();
    this.kafkaParams = HopsUtil.getKafkaProperties().getSparkConsumerConfigMap();
  }

  public SparkConsumer(Collection<String> topics) {
    this.topics = topics;
    this.kafkaParams = HopsUtil.getKafkaProperties().getSparkConsumerConfigMap();
  }

  /**
   *
   * @param jsc
   * @param topics
   */
  public SparkConsumer(JavaStreamingContext jsc, Collection<String> topics) {
    this.jsc = jsc;
    this.topics = topics;
    this.kafkaParams = HopsUtil.getKafkaProperties().getSparkConsumerConfigMap();
  }

  public SparkConsumer(JavaStreamingContext jsc, Collection<String> topics, Properties userProps) {
    this.jsc = jsc;
    this.topics = topics;
    this.kafkaParams = HopsUtil.getKafkaProperties().getSparkConsumerConfigMap(userProps);

  }

  /**
   *
   * @param jsc
   * @param topics
   * @param kafkaParams
   */
  public SparkConsumer(JavaStreamingContext jsc, Collection<String> topics,
      Map<String, Object> kafkaParams) {
    this.jsc = jsc;
    this.topics = topics;
    this.kafkaParams = kafkaParams;
  }

  /**
   *
   * @param <K>
   * @param <V>
   * @return
   */
  public <K extends Object, V extends Object> JavaInputDStream<ConsumerRecord<K, V>> createDirectStream() {
    JavaInputDStream<ConsumerRecord<K, V>> directKafkaStream
        = KafkaUtils.
            createDirectStream(jsc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));
    return directKafkaStream;
  }

  /**
   * Returns a DataStreamReader which the user can then load into the application, for example
   * getKafkaDataStreamReader.load()
   *
   * @return
   */
  public DataStreamReader getKafkaDataStreamReader() {
    sparkSession = SparkSession
        .builder()
        .appName(HopsUtil.getJobName())
        .getOrCreate();

    // Create DataSet representing the stream of input lines from kafka
    return sparkSession
        .readStream()
        .format("kafka")
        .options(HopsUtil.getKafkaProperties().getSparkStructuredStreamingKafkaProps());
  }

  /**
   * Get the spark session after it is initialized by getKafkaDataStreamReader().
   *
   * @return
   */
  public SparkSession getSparkSession() {
    return sparkSession;
  }

  /**
   *
   * @param <K>
   * @param <V>
   * @param ls
   * @return
   */
  public <K extends Object, V extends Object> JavaInputDStream<ConsumerRecord<K, V>> createDirectStream(
      LocationStrategy ls) {
    return KafkaUtils.
        createDirectStream(jsc, ls, ConsumerStrategies.Subscribe(topics,
            kafkaParams));
  }

  public <K extends Object, V extends Object> JavaRDD<ConsumerRecord<K, V>> createRDD(
      JavaSparkContext jsc,
      OffsetRange[] osr, LocationStrategy ls) {
    return KafkaUtils.createRDD(jsc, kafkaParams, osr, ls);
  }

  public void initializeLogIfNecessary(boolean init) {
    KafkaUtils.initializeLogIfNecessary(init);
  }

  public boolean isTraceEnabled() {
    return KafkaUtils.isTraceEnabled();
  }

  public Logger log() {
    return KafkaUtils.log();
  }

  public String logName() {
    return KafkaUtils.logName();
  }

  public void logDebug(Function0<String> fun) {
    KafkaUtils.logDebug(fun);
  }

  public void logDebug(Function0<String> fun, Throwable thr) {
    KafkaUtils.logDebug(fun, thr);
  }

  public void logError(Function0<String> fun) {
    KafkaUtils.logError(fun);
  }

  public void logError(Function0<String> fun, Throwable thr) {
    KafkaUtils.logError(fun, thr);
  }

  public void logInfo(Function0<String> fun) {
    KafkaUtils.logInfo(fun);
  }

  public void logInfo(Function0<String> fun, Throwable thr) {
    KafkaUtils.logInfo(fun, thr);
  }

  public void logTrace(Function0<String> fun) {
    KafkaUtils.logTrace(fun);
  }

  public void logTrace(Function0<String> fun, Throwable thr) {
    KafkaUtils.logTrace(fun, thr);
  }

  public void logWarning(Function0<String> fun) {
    KafkaUtils.logWarning(fun);
  }

  public void logWarning(Function0<String> fun, Throwable thr) {
    KafkaUtils.logWarning(fun, thr);
  }

}
