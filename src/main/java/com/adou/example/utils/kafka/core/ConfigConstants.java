package com.adou.example.utils.kafka.core;

public class ConfigConstants {
	public static final String DEFAULT_GROUP_ID = "default.group";
	public static final Long DEFAULT_AUTO_COMMIT_INTERVAL_MS = Long.valueOf(60000L);

	public static final Integer DEFAULT_NUM_CONSUMER_FETCHERS = Integer.valueOf(1);

	public static final AutoOffsetResetFlag DEFAULT_AUTO_OFFSET_RESET = AutoOffsetResetFlag.LARGEST;

	public static final RequestRequiredAcksFlag DEFAULT_REQUEST_REQUIRE_ACKS = RequestRequiredAcksFlag.WAIT_LEADER_ACKNOWLEDGE;
	public static final int DEFAULT_BATCH_COUNT = 100;
	public static final Long DEFAULT_CONSUMER_TIMEOUT_MS = Long.valueOf(1000L);

	public static enum CommitPolicy {
	}

	public static enum RequestRequiredAcksFlag {
		NEVER_WAIT_ACKNOWLEDGE(Integer.valueOf(0)), WAIT_LEADER_ACKNOWLEDGE(Integer.valueOf(1)), WAIT_ALL_ACKNOWLEDGE(
				Integer.valueOf(-1));

		private Integer statusCode;

		private RequestRequiredAcksFlag(Integer statusCode) {
			this.statusCode = statusCode;
		}

		public Integer getStatusCode() {
			return this.statusCode;
		}

		public static RequestRequiredAcksFlag getFlag(Integer statusCode) {
			for (RequestRequiredAcksFlag flag : values()) {
				if (flag.statusCode.equals(statusCode)) {
					return flag;
				}
			}
			return null;
		}
	}

	public static enum AutoOffsetResetFlag {
		SMALLEST("smallest"), LARGEST("largest");

		private String key;

		private AutoOffsetResetFlag(String key) {
			this.key = key;
		}

		public String getKey() {
			return this.key;
		}

		public static AutoOffsetResetFlag getFlag(String offsetResetFlag) {
			for (AutoOffsetResetFlag flag : values()) {
				if (flag.getKey().equals(offsetResetFlag)) {
					return flag;
				}
			}
			return null;
		}
	}

	public static enum CodecCompressionFlag {
		NONE("none"), GZIP("gzip"), SNAPPY("snappy");

		private String key;

		private CodecCompressionFlag(String key) {
			this.key = key;
		}

		public String getKey() {
			return this.key;
		}
	}

	public static enum ProducersKey {
		ID("id"), TOPIC("topic"), HANDLER_CLASS("handler.class"), KAFKA_BROKER_LIST("kafka.broker.list"), REQUEST_REQUIRED_ACKS(
				"request.required.acks"), EXT_PROPS("ext.props"), POOL_SIZE("producer.pool.size"), ASYNC_ENABLE(
				"send.async.enable");

		private String key;

		private ProducersKey(String key) {
			this.key = key;
		}

		public String getKey() {
			return this.key;
		}
	}

	public static enum ConsumersKey {
		ID("id"), TOPIC("topic"), HANDLER_CLASS("handler.class"), EXECUTOR_SERVICE_CLASS("executor.service.class"), GROUP_ID(
				"group.id"), ZOOKEEPER_CONNECT("zookeeper.connect"), IS_BATCH("is.batch"), BATCH_SIZE("batch.size"), AUTO_COMMIT_ENABLE(
				"auto.commit.enable"), CATCH_EXCEPTION_ENABLE("catch.exception.enable"), AUTO_COMMIT_INTERVAL_MS(
				"auto.commit.interval.ms"), NUM_CONSUMER_FETCHERS("num.consumer.fetchers"), AUTO_OFFSET_RESET(
				"auto.offset.reset"), EXT_PROPS("ext.props");

		private String key;

		private ConsumersKey(String key) {
			this.key = key;
		}

		public String getKey() {
			return this.key;
		}
	}
}
