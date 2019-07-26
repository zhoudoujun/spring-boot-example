package com.adou.example.utils.kafka.consumer.handler;

import java.io.Serializable;
import java.util.List;

public abstract interface IConsumerBatchHandler<V extends Serializable> extends IConsumerHandler<V> {
	public abstract void batchHandle(List<KeyValuePair<V>> paramList);
}
