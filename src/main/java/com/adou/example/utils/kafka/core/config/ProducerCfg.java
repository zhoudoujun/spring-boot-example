package com.adou.example.utils.kafka.core.config;

import com.adou.example.utils.kafka.core.ConfigConstants;
import com.adou.example.utils.kafka.core.ConfigConstants.RequestRequiredAcksFlag;
import com.adou.example.utils.kafka.producer.handler.AbstractProducerHandler;

import java.io.Serializable;
import java.util.Properties;

public class ProducerCfg<V extends Serializable>
{
  private final Properties extProps;
  private final String id;
  private final String topic;
  private final int poolSize;
  private final boolean asyncEnable;
  private final Class<AbstractProducerHandler<V>> handlerClass;
  private final String kafkaBrokers;
  private final ConfigConstants.RequestRequiredAcksFlag requestRequiredAcks;
  private static final int DEFAULT_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;

  public ProducerCfg(Properties extProps, String id, String topic, int poolSize, boolean asyncEnable, Class<AbstractProducerHandler<V>> handlerClass, String kafkaBrokers, ConfigConstants.RequestRequiredAcksFlag requestRequiredAcks)
  {
    this.extProps = extProps;
    this.id = id;
    this.topic = topic;
    this.handlerClass = handlerClass;
    this.kafkaBrokers = kafkaBrokers;
    this.requestRequiredAcks = requestRequiredAcks;
    this.asyncEnable = asyncEnable;
    if (poolSize > 0)
      this.poolSize = poolSize;
    else
      this.poolSize = DEFAULT_POOL_SIZE;
  }

  public Properties getExtProps()
  {
    return (Properties)this.extProps.clone();
  }

  public Class<AbstractProducerHandler<V>> getHandlerClass() {
    return this.handlerClass;
  }

  public String getId() {
    return this.id;
  }

  public String getTopic() {
    return this.topic;
  }

  public String getKafkaBrokers() {
    return this.kafkaBrokers;
  }

  public ConfigConstants.RequestRequiredAcksFlag getRequestRequiredAcks() {
    return this.requestRequiredAcks;
  }

  public int getPoolSize() {
    return this.poolSize;
  }

  public boolean isAsyncEnable() {
    return this.asyncEnable;
  }
}
