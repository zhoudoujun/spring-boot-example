package com.adou.example.kafka.producer;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 封装发送kafka消息
 *
 * @author zhodoujun01
 *
 * Create on 2019年4月26日
 *
 * @param <T>
 */
public class KafkaProducer<T extends Serializable> {
	private static ExecutorService exec = new ThreadPoolExecutor(10, 50, 2L,
            TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
            new ThreadPoolExecutor.CallerRunsPolicy());
	
	public KafkaProducer() {}
	
	public void sendMessage(BasicMessage<T> message) {
		exec.submit(new SendKafka<T>(message.getKey(), message.getSubject(), message.getMessage()));
	}
}
