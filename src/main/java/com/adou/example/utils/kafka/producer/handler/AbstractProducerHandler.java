package com.adou.example.utils.kafka.producer.handler;

import java.io.IOException;
import java.io.Serializable;

import kafka.utils.VerifiableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adou.example.utils.kafka.core.serialize.SerializableUtils;

public abstract class AbstractProducerHandler<V extends Serializable>
  implements IProducerHandler<V>
{
  private static Logger LOG = LoggerFactory.getLogger(AbstractProducerHandler.class);
  protected VerifiableProperties props;

  public AbstractProducerHandler(VerifiableProperties props)
  {
    this.props = props;
  }

  public byte[] toBytes(V paramT)
  {
    if (paramT == null) {
      return new byte[0];
    }
    if ((paramT instanceof String)) {
      String str = (String)paramT;
      return str.getBytes();
    }
    try {
      return SerializableUtils.getBytesFromObject(paramT);
    } catch (IOException e) {
      LOG.error("", e);
      throw new RuntimeException(e);
    }
  }

  public final int partition(Object paramObject, int paramInt)
  {
    String key = (String)paramObject;
    return partition(key, paramInt);
  }

  public abstract int partition(String paramString, int paramInt);
}
