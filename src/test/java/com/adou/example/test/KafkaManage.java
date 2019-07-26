package com.adou.example.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.adou.example.kafka.constant.ConfigFileName;
import com.adou.example.kafka.constant.KafkaSubjectName;
import com.adou.example.kafka.producer.BasicMessage;
import com.adou.example.kafka.producer.KafkaProducer;
import com.adou.example.utils.kafka.consumer.Consumer;
import com.adou.example.utils.kafka.core.config.KafkaClientsConfig;
import com.adou.example.utils.kafka.producer.Producer;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = com.adou.example.TestApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class KafkaManage {

	public static void main(String[] args) throws Exception {
		KafkaProducer<String> producer = new KafkaProducer<>();
		producer.sendMessage(new BasicMessage(KafkaSubjectName.KAFKA_CLIENTS_CFG_SPRING_BOOT_EXAMPLE.getName(), "msg"));
		Thread.sleep(10 * 1000);
		System.out.println("================send success !!!");

		// 查看所有主题
		// KafkaTopicUtil.listAllTopic("47.110.47.104");
		// 创建主题
		// KafkaTopic topic = new KafkaTopic();
		// topic.setTopicName("KAFKA_CLIENTS_CFG_SPRING_BOOT_EXAMPLE"); //
		// topic名称
		// topic.setPartition(1); // 分区数量设置为1
		// topic.setReplication(1); // 副本数量设置为1
		// KafkaUtil.createKafaTopic("47.110.47.104", topic);
	}

	// 发送消息
	@Test
	public void send() throws Exception {
		KafkaClientsConfig config = new KafkaClientsConfig(
				ConfigFileName.KAFKA_CLIENTS_CFG_SPRING_BOOT_EXAMPLE.getName());
		Producer<String> producer = (Producer<String>) config
				.getOrCreateProducer(KafkaSubjectName.KAFKA_CLIENTS_CFG_SPRING_BOOT_EXAMPLE.getName());
		producer.send("key", "msg");
		System.out.println("================= send end……");
		Thread.sleep(5000);
	}

	// 消费消息
	// @Test
	public void receive() throws Exception {
		KafkaClientsConfig config = new KafkaClientsConfig(ConfigFileName.KAFKA_CLIENTS_CFG_SPRING_BOOT_EXAMPLE.getName());
		//
		Consumer<?> consumer = config.newConsumer(KafkaSubjectName.KAFKA_CLIENTS_CFG_SPRING_BOOT_EXAMPLE.getName());
		consumer.startReceive();

		System.out.println("================= receive start……");

		Thread.sleep(5 * 1000);
	}
}
