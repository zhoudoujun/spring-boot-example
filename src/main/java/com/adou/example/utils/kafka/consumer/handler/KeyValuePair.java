package com.adou.example.utils.kafka.consumer.handler;

public class KeyValuePair<V> {
	private String key;
	private V value;

	public KeyValuePair(String key, V value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return this.key;
	}

	public V getValue() {
		return this.value;
	}
}
