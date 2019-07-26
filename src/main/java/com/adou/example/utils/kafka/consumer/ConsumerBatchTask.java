package com.adou.example.utils.kafka.consumer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adou.example.utils.kafka.consumer.handler.IConsumerBatchHandler;
import com.adou.example.utils.kafka.consumer.handler.KeyValuePair;
import com.adou.example.utils.kafka.core.ConfigConstants;

public class ConsumerBatchTask<V extends Serializable>
  implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(ConsumerBatchTask.class);
  private KafkaStream<String, V> m_stream;
  private IConsumerBatchHandler<V> m_batch_handler;
  private int m_batch_size;
  private boolean m_auto_commit_enable;
  private List<KeyValuePair<V>> m_msg_cache;
  private Long consumerTimeoutMS;
  private long lastStartBatchTime;
  private boolean catchExceptionEnable;

  public ConsumerBatchTask(KafkaStream<String, V> stream, IConsumerBatchHandler<V> handler, int batchSize, Long consumerTimeoutMS, boolean autoCommitEnable, boolean catchExceptionEnable)
  {
    this.m_stream = stream;
    this.m_batch_handler = handler;
    this.m_batch_size = batchSize;
    this.m_auto_commit_enable = autoCommitEnable;
    this.catchExceptionEnable = catchExceptionEnable;
    this.consumerTimeoutMS = consumerTimeoutMS;
    if (this.consumerTimeoutMS == null)
      this.consumerTimeoutMS = ConfigConstants.DEFAULT_CONSUMER_TIMEOUT_MS;
  }

  public void run()
  {
    try
    {
      ConsumerIterator consumerIte = this.m_stream.iterator();
      this.lastStartBatchTime = System.currentTimeMillis();
      while (true)
      {
        if (hasNext(consumerIte)) {
          MessageAndMetadata messageAndMetaData = consumerIte.next();

          KeyValuePair msg = new KeyValuePair(
            (String)messageAndMetaData
            .key(), messageAndMetaData.message());
          if (this.m_msg_cache == null) {
            this.m_msg_cache = new ArrayList();
          }
          this.m_msg_cache.add(msg);
          if (this.m_msg_cache.size() >= this.m_batch_size)
            doBatchHandler();
          else if (isWaitTimeOut())
            doBatchHandler();
        }
        else {
          LOG.debug("consumer timeout!");
          if (this.m_msg_cache == null) {
            this.m_msg_cache = new ArrayList();
          }
          doBatchHandler();
        }
      }
    } catch (Throwable e) {
      LOG.error("", e);
      throw new RuntimeException(e);
    }
  }

  private boolean isWaitTimeOut() {
    long now = System.currentTimeMillis();
    long costTime = now - this.lastStartBatchTime;
    if (costTime >= this.consumerTimeoutMS.longValue()) {
      LOG.debug("wait batch cost time:" + costTime + "ms");
      return true;
    }
    return false;
  }

  private boolean hasNext(ConsumerIterator<String, V> consumerIte)
  {
    try
    {
      return consumerIte.hasNext(); } catch (ConsumerTimeoutException e) {
    }
    return false;
  }

  private void doBatchHandler()
  {
    if ((this.m_msg_cache == null) || (this.m_msg_cache.size() == 0)) {
      LOG.debug("no data in batch cache");
      this.lastStartBatchTime = System.currentTimeMillis();
      return;
    }
    List messages = this.m_msg_cache;
    this.m_msg_cache = new ArrayList();
    try {
      this.m_batch_handler.batchHandle(messages);
      if (this.m_auto_commit_enable) {
        LOG.debug("auto.commit.enable is true,do commit");
        this.m_batch_handler.getConsumer().commit();
      }
      this.lastStartBatchTime = System.currentTimeMillis();
    } catch (Exception e) {
      LOG.error("", e);
      if (!this.catchExceptionEnable)
        throw new RuntimeException(e);
    }
  }
}
