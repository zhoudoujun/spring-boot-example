package com.adou.example.kafka.constant;

public enum KafkaSubjectName {
	KAFKA_CLIENTS_CFG_SPRING_BOOT_EXAMPLE("KAFKA_CLIENTS_CFG_SPRING_BOOT_EXAMPLE");
	
	private KafkaSubjectName(String name) {
		this.name = name;
	}

	private String name;

	public String getName() {
		return name;
	}
}
