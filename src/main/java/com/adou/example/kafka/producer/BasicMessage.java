package com.adou.example.kafka.producer;

import java.io.Serializable;

/**
 * 发送的kafka消息封装
 *
 * @author zhodoujun01
 *
 * Create on 2019年4月26日
 *
 * @param <T>
 */
public class BasicMessage<T extends Serializable> {
	private String key;
	private String subject;
	private T message;
	
	public BasicMessage(String subject, T message) {
		this.key = null;
		this.subject = subject;
		this.message = message;
	}
	
	public BasicMessage(String key, String subject, T message) {
		this.key = key;
		this.subject = subject;
		this.message = message;
	}
	
	public String getKey() {
		return key;
	}

	public String getSubject() {
		return this.subject;
	}
	
	public T getMessage() {
		return this.message;
	}

	@Override
	public String toString() {
		return "BasicMessage [key=" + key + ", subject=" + subject + ", message=" + message + "]";
	}
}
