package com.adou.example.kafka.consumer;

import kafka.utils.VerifiableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adou.example.utils.kafka.consumer.handler.AbstractConsumerHandler;



public class ExampleAsyncConsumer extends AbstractConsumerHandler<String> {
	private static final Logger LOG = LoggerFactory.getLogger(ExampleAsyncConsumer.class);
	public ExampleAsyncConsumer(VerifiableProperties props) {
		super(props);
	}

	@Override
	public void handle(String paramString, Object paramV) {
		LOG.info("==================== ExampleAsyncConsumer paramString:{}, paramV:{}", paramString, paramV);
	}
}
