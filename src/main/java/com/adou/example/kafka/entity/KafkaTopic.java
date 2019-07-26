package com.adou.example.kafka.entity;

/**
 * kafka主题实体
 * 
 * @author zhoudoujun01
 * @date 2019年7月17日16:24:04
 */
public class KafkaTopic {
	private String topicName; // topic 名称
	private Integer partition; // partition 分区数量
	private Integer replication; // replication 副本数量
	private String descrbe;

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public Integer getPartition() {
		return partition;
	}

	public void setPartition(Integer partition) {
		this.partition = partition;
	}

	public Integer getReplication() {
		return replication;
	}

	public void setReplication(Integer replication) {
		this.replication = replication;
	}

	public String getDescrbe() {
		return descrbe;
	}

	public void setDescrbe(String descrbe) {
		this.descrbe = descrbe;
	}

	@Override
	public String toString() {
		return "KafkaTopicBean [topicName=" + topicName + ", partition=" + partition + ", replication=" + replication
				+ ", descrbe=" + descrbe + "]";
	}

}
