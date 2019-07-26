package com.adou.example.kafka.constant;

public enum ConfigFileName {
	KAFKA_CLIENTS_CFG_SPRING_BOOT_EXAMPLE("kafka-clients-cfg.xml");
	
	private String name;

	private ConfigFileName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
}
