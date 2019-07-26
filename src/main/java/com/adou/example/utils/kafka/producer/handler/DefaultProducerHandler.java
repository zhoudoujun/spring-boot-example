package com.adou.example.utils.kafka.producer.handler;

import java.io.Serializable;
import java.util.Random;

import kafka.utils.VerifiableProperties;

public class DefaultProducerHandler<V extends Serializable> extends AbstractProducerHandler<V> {
	public DefaultProducerHandler(VerifiableProperties props) {
		super(props);
	}

	public int partition(String key, int partitionsCount) {
		if ((key == null) || ("".equals(key.trim()))) {
			Random random = new Random();
			return random.nextInt(partitionsCount);
		}
		return Math.abs(key.hashCode()) % partitionsCount;
	}
}
