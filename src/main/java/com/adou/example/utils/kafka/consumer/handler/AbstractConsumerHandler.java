package com.adou.example.utils.kafka.consumer.handler;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;

import kafka.utils.VerifiableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adou.example.utils.kafka.consumer.Consumer;
import com.adou.example.utils.kafka.core.serialize.SerializableUtils;

public abstract class AbstractConsumerHandler<V extends Serializable> implements IConsumerHandler<V> {
	private static Logger LOG = LoggerFactory.getLogger(AbstractConsumerHandler.class);
	protected VerifiableProperties props;
	protected Consumer<V> consumer;
	protected Class<V> vCls;

	public AbstractConsumerHandler(VerifiableProperties props) {
		this.props = props;
		ParameterizedType parameterizedType = (ParameterizedType) getClass().getGenericSuperclass();
		this.vCls = ((Class) parameterizedType.getActualTypeArguments()[0]);
		LOG.info(getClass().getName() + " is " + this.vCls.getName() + "'s consumer handler");
	}

	public V fromBytes(byte[] paramArrayOfByte) {
		if (String.class.equals(this.vCls))
			return (V) new String(paramArrayOfByte);
		try {
			return (V) SerializableUtils.getObjectFromBytes(paramArrayOfByte);
		} catch (IOException e) {
			LOG.error("", e);
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			LOG.error("", e);
			throw new RuntimeException(e);
		}
	}

	protected void commit() {
		if (this.consumer != null)
			this.consumer.commit();
	}

	public Consumer<V> getConsumer() {
		return this.consumer;
	}

	public void setConsumer(Consumer<V> consumer) {
		this.consumer = consumer;
	}
}
