package com.adou.example.utils.kafka.consumer;

import java.io.Serializable;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adou.example.utils.kafka.consumer.handler.IConsumerHandler;

public class ConsumerTask<V extends Serializable> implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(ConsumerTask.class);
	private KafkaStream<String, V> m_stream;
	private IConsumerHandler<V> m_handler;
	private boolean catchExceptionEnable;

	public ConsumerTask(KafkaStream<String, V> stream, IConsumerHandler<V> handler, boolean catchExceptionEnable) {
		this.m_stream = stream;
		this.m_handler = handler;
		this.catchExceptionEnable = catchExceptionEnable;
	}

	public void run() {
		try {
			ConsumerIterator consumerIte = this.m_stream.iterator();
			while (consumerIte.hasNext()) {
				MessageAndMetadata messageAndMetaData = consumerIte.next();
				try {
					this.m_handler.handle((String) messageAndMetaData.key(),
							 messageAndMetaData.message());
				} catch (Exception e) {
					LOG.error("", e);
					if (!this.catchExceptionEnable)
						throw new RuntimeException(e);
				}
			}
		} catch (Throwable e) {
			LOG.error("", e);
			throw new RuntimeException(e);
		}
	}
}