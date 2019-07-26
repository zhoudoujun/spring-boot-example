package com.adou.example.utils.kafka.producer;

import com.adou.example.utils.kafka.core.ConfigConstants;
import com.adou.example.utils.kafka.core.ConfigConstants.CodecCompressionFlag;
import com.adou.example.utils.kafka.core.ConfigConstants.RequestRequiredAcksFlag;
import com.adou.example.utils.kafka.core.config.ProducerCfg;
import com.adou.example.utils.kafka.producer.handler.AbstractProducerHandler;
import com.adou.example.utils.kafka.producer.handler.DefaultProducerHandler;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import kafka.utils.VerifiableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer<V extends Serializable>
{
  private Properties producerProps;
  private String id;
  private String topic;
  private AbstractProducerHandler<V> handler;
  private String kafkaBrokers;
  private ConfigConstants.RequestRequiredAcksFlag requestRequiredAcks;
  private kafka.javaapi.producer.Producer<String, V> producer;
  private boolean asyncEnable;
  private List<KeyedMessage<String, V>> messageCache;
  private ExecutorService executors;
  private BlockingQueue<Runnable> blockingQueue;
  private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

  public static String METADATA_BROKER_LIST_KEY = "metadata.broker.list";
  public static String REQUEST_REQUIRED_ACKS_KEY = "request.required.acks";
  public static String VALUE_ENCODER_KEY = "serializer.class";
  public static String KEY_ENCODER_KEY = "key.serializer.class";
  public static String PARTITIONER_CLASS_KEY = "partitioner.class";
  public static String COMPRESSION_CODEC = "compression.codec";
  public static String COMPRESSION_TOPIC = "compressed.topics";

  private Object cacheLocker = new Object();

  private Object sendLocker = new Object();

  public Producer(ProducerCfg<V> producerCfg, VerifiableProperties verifiableProps)
    throws IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
  {
    this(producerCfg.getId(), producerCfg
      .getTopic(), producerCfg
      .isAsyncEnable(), producerCfg
      .getHandlerClass() == null ? null : 
      (AbstractProducerHandler)producerCfg
      .getHandlerClass()
      .getConstructor(new Class[] { VerifiableProperties.class })
      .newInstance(new Object[] { verifiableProps }), 
      producerCfg
      .getKafkaBrokers(), producerCfg
      .getRequestRequiredAcks(), producerCfg
      .getExtProps());
  }

  public Producer(String id, String topic, boolean asyncEnable, AbstractProducerHandler<V> handler, String kafkaBrokers, ConfigConstants.RequestRequiredAcksFlag requestRequiredAcks, Properties producerProps)
  {
    this.producerProps = producerProps;
    if (this.producerProps == null) {
      this.producerProps = new Properties();
    }

    if ((id == null) || ("".equals(id.trim()))) {
      throw new NullPointerException("Producer has not 'id' property");
    }
    this.id = id;

    if ((topic == null) || ("".equals(topic.trim()))) {
      throw new NullPointerException("Producer has not 'topic' property");
    }
    this.topic = topic;

    if (handler == null)
      this.handler = new DefaultProducerHandler(null);
    else {
      this.handler = handler;
    }

    if ((kafkaBrokers == null) || ("".equals(kafkaBrokers.trim()))) {
      throw new NullPointerException("Producer has not 'kafka.Broker.list' property");
    }
    this.kafkaBrokers = kafkaBrokers;

    if (requestRequiredAcks == null)
      this.requestRequiredAcks = ConfigConstants.DEFAULT_REQUEST_REQUIRE_ACKS;
    else {
      this.requestRequiredAcks = requestRequiredAcks;
    }

    this.asyncEnable = asyncEnable;

    Runtime.getRuntime().addShutdownHook(new ShutdownThread());
    reload();

    if (this.asyncEnable)
    {
      this.messageCache = new ArrayList();
      this.blockingQueue = new ArrayBlockingQueue(10);
      this.executors = new ThreadPoolExecutor(1, 1, 1L, TimeUnit.MINUTES, this.blockingQueue);
    }
  }

  private void reloadProps()
  {
    if (this.producerProps == null) {
      this.producerProps = new Properties();
    }
    this.producerProps.put(METADATA_BROKER_LIST_KEY, this.kafkaBrokers);
    this.producerProps.put(REQUEST_REQUIRED_ACKS_KEY, this.requestRequiredAcks.getStatusCode().toString());
    reloadKeyEncoderClass();
    reloadValueEncoderClass();
    reloadPartitionerClass();
    reloadCompressionInfo();
  }

  private void reloadCompressionInfo()
  {
    if ((this.producerProps != null) && (!this.producerProps.containsKey(COMPRESSION_CODEC))) {
      this.producerProps.put(COMPRESSION_CODEC, ConfigConstants.CodecCompressionFlag.GZIP.getKey());
      this.producerProps.put(COMPRESSION_TOPIC, this.topic);
    }
  }

  private void reloadKeyEncoderClass()
  {
    this.producerProps.put(KEY_ENCODER_KEY, StringEncoder.class.getName());
  }

  private void reloadValueEncoderClass()
  {
    this.producerProps.put(VALUE_ENCODER_KEY, this.handler.getClass().getName());
  }

  private void reloadPartitionerClass()
  {
    this.producerProps.put(PARTITIONER_CLASS_KEY, this.handler.getClass().getName());
  }

  public void reload()
  {
    closeConnection();
    reloadProps();
    ProducerConfig config = new ProducerConfig(this.producerProps);
    this.producer = new kafka.javaapi.producer.Producer(config);
  }

  private List<KeyedMessage<String, V>> getMsgToSend()
  {
    synchronized (this.cacheLocker) {
      List messages = this.messageCache;

      this.messageCache = new ArrayList();
      return messages;
    }
  }

  private void closeConnection()
  {
    if (this.producer != null)
      this.producer.close();
  }

  public void send(String key, V value)
  {
    if (this.producer == null) {
      reload();
    }
    if (key == null) {
      key = "";
    }
    KeyedMessage kafkaKeyedMessage = new KeyedMessage(this.topic, key, value);
    doSend(kafkaKeyedMessage);
  }

  public void batchSend(List<Message<V>> messages)
  {
    if (this.producer == null) {
      reload();
    }
    List kafkasMessages = new ArrayList();

    for (Message message : messages) {
      kafkasMessages.add(convertToKafkasKeyedMessage(message));
    }
    doBatchSend(kafkasMessages);
  }

  public void send(Message<V> message)
  {
    if (this.producer == null) {
      reload();
    }
    KeyedMessage kafkaKeyedMessage = convertToKafkasKeyedMessage(message);
    doSend(kafkaKeyedMessage);
  }

  private void doAsyncSend()
  {
    synchronized (this.sendLocker) {
      if (this.messageCache.size() >= 10000)
        try {
          Thread.sleep(10L);
        }
        catch (InterruptedException localInterruptedException) {
        }
      if (this.blockingQueue.size() <= 5)
        this.executors.submit(new AsyncSendThread());
    }
  }

  private void doBatchSend(List<KeyedMessage<String, V>> msgs)
  {
    if (this.asyncEnable) {
      synchronized (this.cacheLocker) {
        this.messageCache.addAll(msgs);
      }
      doAsyncSend();
    } else {
      try {
        this.producer.send(msgs);
      } catch (Exception e) {
        LOG.error("", e);
        throw new RuntimeException(e);
      }
    }
  }

  private void doSend(KeyedMessage<String, V> msg) {
    if (this.asyncEnable) {
      synchronized (this.cacheLocker) {
        this.messageCache.add(msg);
      }
      doAsyncSend();
    } else {
      try {
        this.producer.send(msg);
      } catch (Exception e) {
        LOG.error("", e);
        throw new RuntimeException(e);
      }
    }
  }

  private KeyedMessage<String, V> convertToKafkasKeyedMessage(Message<V> message)
  {
    return new KeyedMessage(this.topic, message
      .getKey(), message
      .getValue());
  }

  public String getProducerProp(String key) {
    return this.producerProps.getProperty(key);
  }

  public Properties getProducerProps() {
    return this.producerProps;
  }

  public void setProducerProps(Properties producerProps) {
    this.producerProps = producerProps;
  }

  public String getId() {
    return this.id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getTopic() {
    return this.topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public AbstractProducerHandler<V> getHandler() {
    return this.handler;
  }

  public void setHandler(AbstractProducerHandler<V> handler) {
    this.handler = handler;
  }

  public String getKafkaBrokers() {
    return this.kafkaBrokers;
  }

  public void setKafkaBrokers(String kafkaBrokers) {
    this.kafkaBrokers = kafkaBrokers;
  }

  public ConfigConstants.RequestRequiredAcksFlag getRequestRequiredAcks() {
    return this.requestRequiredAcks;
  }

  public void setRequestRequiredAcks(ConfigConstants.RequestRequiredAcksFlag requestRequiredAcks) {
    this.requestRequiredAcks = requestRequiredAcks;
  }

  public kafka.javaapi.producer.Producer<String, V> getProducer() {
    return this.producer;
  }

  public void setProducer(kafka.javaapi.producer.Producer<String, V> producer) {
    this.producer = producer;
  }

  public class AsyncSendThread
    implements Runnable
  {
    public AsyncSendThread()
    {
    }

    public void run()
    {
      if ((Producer.this.messageCache == null) || (Producer.this.messageCache.size() == 0)) {
        return;
      }
      List msgToSend = Producer.this.getMsgToSend();
      if ((msgToSend != null) && (msgToSend.size() > 0)) {
        Producer.this.producer.send(msgToSend);
        Producer.LOG.info("async send " + msgToSend.size() + " msgs");
      }
    }
  }

  public class ShutdownThread extends Thread
  {
    public ShutdownThread()
    {
    }

    public void run()
    {
      Producer.LOG.info("close producer with id: " + Producer.this.id + " and topic: " + Producer.this.topic);
      if (Producer.this.asyncEnable) {
        Producer.this.asyncEnable = false;
        Producer.this.executors.shutdown();
        if ((Producer.this.messageCache != null) && (Producer.this.messageCache.size() > 0)) {
          Producer.LOG.info("send surplus " + Producer.this.messageCache.size() + " messages before close!");
          Producer.this.producer.send(Producer.this.messageCache);
        }
      }
      Producer.this.closeConnection();
    }
  }
}