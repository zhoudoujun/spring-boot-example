package com.adou.example.utils;

import java.util.List;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;

import org.apache.kafka.common.security.JaasUtils;

import scala.collection.JavaConversions;

import com.adou.example.kafka.entity.KafkaTopic;

/**
 * 主题管理类
 * 
 * @author zhoudoujun01
 * @date 2019年7月17日16:24:04
 */
public class KafkaTopicUtil {

	/**
	 * 创建主题
	 * 
	 * @param ZkStr
	 * @param topic
	 */
	public static void createKafaTopic(String ZkStr, KafkaTopic topic) {
		ZkUtils zkUtils = ZkUtils.apply(ZkStr, 30000, 30000, JaasUtils.isZkSecurityEnabled());

		AdminUtils.createTopic(zkUtils, topic.getTopicName(), topic.getPartition(), topic.getReplication(),
				new Properties(), new RackAwareMode.Enforced$());
		zkUtils.close();
	}

	/**
	 * 删除主题
	 * 
	 * @param ZkStr
	 * @param topic
	 */
	public static void deleteKafaTopic(String ZkStr, KafkaTopic topic) {
		ZkUtils zkUtils = ZkUtils.apply(ZkStr, 30000, 30000, JaasUtils.isZkSecurityEnabled());

		AdminUtils.deleteTopic(zkUtils, topic.getTopicName());
		zkUtils.close();
	}

	/**
	 * 查看所有主题
	 * 
	 * @param zkUrl
	 */
	public static void listAllTopic(String zkUrl) {
		ZkUtils zkUtils = null;
		try {
			zkUtils = ZkUtils.apply(zkUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled());

			List<String> topics = JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
			System.out.println("----------------------- topics start");
			topics.forEach(System.out::println);
			System.out.println("----------------------- topics end");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (zkUtils != null) {
				zkUtils.close();
			}
		}
	}

}
