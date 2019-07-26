package com.adou.example.kafka.producer;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adou.example.utils.kafka.core.config.KafkaClientsConfig;
import com.adou.example.utils.kafka.producer.Producer;
import com.adou.example.utils.DesensitizationUtil;
import com.adou.example.kafka.constant.ConfigFileName;
import com.adou.example.kafka.constant.KafkaSubjectName;


/**
 * 封装发送kafka消息-线程
 *
 * @author zhodoujun01
 *
 * Create on 2019年4月26日
 *
 * @param <T>
 */
public class SendKafka<T extends Serializable> implements Runnable{
	private static final Logger LOG = LoggerFactory.getLogger(SendKafka.class);
	
	private String key;
	private String subject;
	private T msg;
	
	public SendKafka(final String key, final String subject, final T msg) {
		this.key = key;
		this.subject = subject;
		this.msg = msg;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void run() {
		long beginTime = System.currentTimeMillis();
		// 需要脱敏的消息
//		if(KafkaSubjectName.KAFKA_CLIENTS_CFG_SPRING_BOOT_EXAMPLE.getName().equals(subject)) {
//			LOG.info("sendKafka start...... key= " + key +",subject=" + subject + ", msg=" + DesensitizationUtil.getDesensitizationData((String)msg, "consignee","consigneeAddress","buyerName","consigneePhone"));
//		}else {
//			LOG.info("sendKafka start...... key= " + key +",subject=" + subject + ", msg=" + msg);
//		}
		
		try {
            KafkaClientsConfig config = new KafkaClientsConfig(ConfigFileName.KAFKA_CLIENTS_CFG_SPRING_BOOT_EXAMPLE.getName());
            Producer<T> producer = (Producer<T>) config.getOrCreateProducer(subject);
            producer.send(key, msg);
        } catch (Exception e) {
        	LOG.error("sendKafka fail! ", e);
        }
		LOG.info("sendKafka end, subject=" + subject + ", cost " + (System.currentTimeMillis() - beginTime) + "ms");
	}
}
