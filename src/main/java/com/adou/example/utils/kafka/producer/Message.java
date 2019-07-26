package com.adou.example.utils.kafka.producer;

import java.io.Serializable;

public class Message<V extends Serializable> {
	private String key;
	private V value;

	public Message(String key, V value) {
		this.key = key;
		if (this.key == null) {
			this.key = "";
		}
		this.value = value;
	}

	public String getKey() {
		return this.key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public V getValue() {
		return this.value;
	}

	public void setValue(V value) {
		this.value = value;
	}
}
