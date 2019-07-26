package com.adou.example.utils.kafka.consumer.handler;

import java.io.Serializable;

import kafka.serializer.Decoder;

import com.adou.example.utils.kafka.consumer.Consumer;

public abstract interface IConsumerHandler<V extends Serializable> extends Decoder<V> {
	public abstract void handle(String paramString, Object paramV);

	public abstract Consumer<V> getConsumer();

	public abstract void setConsumer(Consumer<V> paramConsumer);
}
