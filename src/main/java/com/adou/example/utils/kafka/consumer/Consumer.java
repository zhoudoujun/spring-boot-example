package com.adou.example.utils.kafka.consumer;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adou.example.utils.kafka.consumer.handler.IConsumerBatchHandler;
import com.adou.example.utils.kafka.consumer.handler.IConsumerHandler;
import com.adou.example.utils.kafka.core.ConfigConstants;
import com.adou.example.utils.kafka.core.config.ConsumerCfg;
import com.adou.example.utils.kafka.core.executor.IKafkaClientsExecutorService;
import com.adou.example.utils.kafka.core.executor.ThreadPoolExecutorService;

public class Consumer<V extends Serializable> {
	private static Logger LOG = LoggerFactory.getLogger(Consumer.class);
	private ConsumerConnector connection;
	private Properties consumerProps;
	private String id;
	private String topic;
	private IConsumerHandler<V> handler;
	private String groupID;
	private String zookeeperConnect;
	private Boolean autoCommitEnable;
	private Boolean catchExceptionEnable = Boolean.valueOf(true);
	private Boolean isBatch;
	private Integer batchSize;
	private Long autoCommitIntervalMS;
	private Integer numConsumerFetchers;
	private ConfigConstants.AutoOffsetResetFlag autoOffsetResetFlag;
	private IKafkaClientsExecutorService kafkaClientsExecutorService;
	public static final String KEY_GROUP_ID = "group.id";
	public static final String KEY_ZOOKEEPER_CONNECT = "zookeeper.connect";
	public static final String KEY_AUTO_COMMIT_ENABLE = "auto.commit.enable";
	public static final String KEY_AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";
	public static final String KEY_NUM_CONSUMER_FETCHER = "num.consumer.fetchers";
	public static final String KEY_AUTO_OFFSET_RESET = "auto.offset.reset";
	public static final String KEY_CONSUMER_TIMEOUT_MS = "consumer.timeout.ms";

	public Consumer(ConsumerCfg<V> consumerCfg, VerifiableProperties verifiableProps) throws IllegalArgumentException,
			SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException,
			NoSuchMethodException {
		this(consumerCfg.getId(), consumerCfg.getTopic(), consumerCfg.getHandlerClass() == null ? null : consumerCfg
				.getHandlerInstance(verifiableProps), consumerCfg.getGroupID(), consumerCfg.getZookeeperConnect(),
				consumerCfg.getAutoCommitEnable(), consumerCfg.getCatchExceptionEnable(), consumerCfg.getIsBatch(),
				consumerCfg.getBatchSize(), consumerCfg.getAutoCommitIntervalMS(),
				consumerCfg.getNumConsumerFetchers(), consumerCfg.getAutoOffsetResetFlag(), consumerCfg.getExtProps(),
				consumerCfg.getKafkaClientsExecutorServiceClass() == null ? null
						: (IKafkaClientsExecutorService) consumerCfg.getKafkaClientsExecutorServiceClass()
								.newInstance());
	}

	public Consumer(String id, String topic, IConsumerHandler<V> handler, String groupID, String zookeeperConnect,
			Boolean autoCommitEnable, Boolean catchExceptionEnable, Boolean isBatch, Integer batchSize,
			Long autoCommitIntervalMS, Integer numConsumerFetchers,
			ConfigConstants.AutoOffsetResetFlag autoOffsetResetFlag, Properties consumerProps,
			IKafkaClientsExecutorService kafkaClientsExecutorService) {
		this.consumerProps = consumerProps;
		if (this.consumerProps == null) {
			this.consumerProps = new Properties();
		}

		if ((id == null) || ("".equals(id.trim()))) {
			throw new NullPointerException("Consumer has not 'id' property");
		}
		this.id = id;

		if ((topic == null) || ("".equals(topic.trim()))) {
			throw new NullPointerException("Consumer has not 'topic' property");
		}
		this.topic = topic;

		if (handler == null) {
			throw new NullPointerException("Consumer has not 'handler' property");
		}
		this.handler = handler;
		this.handler.setConsumer(this);

		if ((groupID == null) || ("".equals(groupID.trim())))
			this.groupID = "default.group";
		else {
			this.groupID = groupID;
		}

		if ((zookeeperConnect == null) || ("".equals(zookeeperConnect.trim()))) {
			throw new NullPointerException("Consumer has not 'zookeeper.connect' property");
		}
		this.zookeeperConnect = zookeeperConnect;

		this.autoCommitEnable = autoCommitEnable;
		if (catchExceptionEnable != null) {
			this.catchExceptionEnable = catchExceptionEnable;
		}
		if ((autoCommitIntervalMS == null) || (autoCommitIntervalMS.longValue() <= 0L))
			this.autoCommitIntervalMS = ConfigConstants.DEFAULT_AUTO_COMMIT_INTERVAL_MS;
		else {
			this.autoCommitIntervalMS = autoCommitIntervalMS;
		}

		if (isBatch == null)
			this.isBatch = Boolean.valueOf(false);
		else {
			this.isBatch = isBatch;
		}

		if ((batchSize == null) || (batchSize.intValue() <= 0))
			this.batchSize = Integer.valueOf(100);
		else {
			this.batchSize = batchSize;
		}

		if ((numConsumerFetchers == null) || (numConsumerFetchers.intValue() <= 0))
			this.numConsumerFetchers = ConfigConstants.DEFAULT_NUM_CONSUMER_FETCHERS;
		else {
			this.numConsumerFetchers = numConsumerFetchers;
		}

		if (autoOffsetResetFlag == null)
			this.autoOffsetResetFlag = ConfigConstants.DEFAULT_AUTO_OFFSET_RESET;
		else {
			this.autoOffsetResetFlag = autoOffsetResetFlag;
		}

		if (kafkaClientsExecutorService == null)
			this.kafkaClientsExecutorService = new ThreadPoolExecutorService();
		else {
			this.kafkaClientsExecutorService = kafkaClientsExecutorService;
		}
		reload();
	}

	private Decoder<String> getKeyDecoder() {
		return new StringDecoder(null);
	}

	private Decoder<V> getValueDecoder() {
		return this.handler;
	}

	private void reloadProperties() {
		if (this.consumerProps == null) {
			this.consumerProps = new Properties();
		}
		this.consumerProps.put("group.id", this.groupID);
		this.consumerProps.put("zookeeper.connect", this.zookeeperConnect);
		this.consumerProps.put("auto.commit.enable", this.autoCommitEnable.toString());
		this.consumerProps.put("auto.commit.interval.ms", this.autoCommitIntervalMS.toString());
		this.consumerProps.put("num.consumer.fetchers", this.numConsumerFetchers.toString());
		this.consumerProps.put("auto.offset.reset", this.autoOffsetResetFlag.getKey());
		if ((this.isBatch.booleanValue()) && (!this.consumerProps.containsKey("consumer.timeout.ms"))) {
			this.consumerProps.put("consumer.timeout.ms", ConfigConstants.DEFAULT_CONSUMER_TIMEOUT_MS.toString());
		}
		if (this.isBatch.booleanValue())
			this.consumerProps.put("auto.commit.enable", Boolean.FALSE.toString());
		else
			this.consumerProps.remove("consumer.timeout.ms");
	}

	public void reload() {
		reloadProperties();
		if (this.connection != null) {
			this.connection.shutdown();
		}
		this.kafkaClientsExecutorService.reload();
		ConsumerConfig consumerConfig = new ConsumerConfig(this.consumerProps);
		this.connection = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
	}

	private void execute() {
		Map topicCountMap = new HashMap();

		topicCountMap.put(this.topic, this.numConsumerFetchers);
		TopicFilter topicFilter = new Whitelist(this.topic);

		List<KafkaStream<String, V>>  streams = this.connection.createMessageStreamsByFilter(topicFilter, this.numConsumerFetchers.intValue(),
				getKeyDecoder(), getValueDecoder());
		IConsumerBatchHandler batchHandler;
		Long consumerTimeourMS;
		if ((this.isBatch.booleanValue()) && ((this.handler instanceof IConsumerBatchHandler))) {
			batchHandler = (IConsumerBatchHandler<Serializable>) this.handler;
			consumerTimeourMS = Long.valueOf(Long.parseLong(this.consumerProps.get("consumer.timeout.ms").toString()));
			for (KafkaStream stream : streams)
				this.kafkaClientsExecutorService.execute(new ConsumerBatchTask(stream, batchHandler, this.batchSize
						.intValue(), consumerTimeourMS, this.autoCommitEnable.booleanValue(), this.catchExceptionEnable
						.booleanValue()));
		} else {
			for (KafkaStream stream : streams)
				this.kafkaClientsExecutorService.execute(new ConsumerTask(stream, this.handler,
						this.catchExceptionEnable.booleanValue()));
		}
	}

	private void shutdown() {
		if (this.connection != null) {
			this.connection.shutdown();
		}
		if (this.kafkaClientsExecutorService != null)
			this.kafkaClientsExecutorService.shutdown();
	}

	public void commit() {
		if (this.connection != null) {
			LOG.debug("do commit ...");
			this.connection.commitOffsets();
		}
	}

	public void startReceive(long maxWaitSeconds) {
		execute();
		if (maxWaitSeconds > 0L)
			try {
				Thread.sleep(maxWaitSeconds * 1000L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				shutdown();
			}
		else
			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					Consumer.this.shutdown();
				}
			});
	}

	public void startReceive() {
		startReceive(-1L);
	}

	public IConsumerHandler<V> getHandler() {
		return this.handler;
	}

	public void setHandler(IConsumerHandler<V> handler) {
		this.handler = handler;
	}

	public String getConsumerProp(String key) {
		return this.consumerProps.getProperty(key);
	}

	public Properties getConsumerProps() {
		return this.consumerProps;
	}

	public void setConsumerProp(Properties consumerProps) {
		this.consumerProps = consumerProps;
	}

	public String getId() {
		return this.id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTopic() {
		return this.topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public ConsumerConnector getConnection() {
		return this.connection;
	}

	public void setConnection(ConsumerConnector connection) {
		this.connection = connection;
	}

	public String getGroupID() {
		return this.groupID;
	}

	public void setGroupID(String groupID) {
		this.groupID = groupID;
	}

	public String getZookeeperConnect() {
		return this.zookeeperConnect;
	}

	public void setZookeeperConnect(String zookeeperConnect) {
		this.zookeeperConnect = zookeeperConnect;
	}

	public Boolean getAutoCommitEnable() {
		return this.autoCommitEnable;
	}

	public void setAutoCommitEnable(Boolean autoCommitEnable) {
		this.autoCommitEnable = autoCommitEnable;
	}

	public Long getAutoCommitIntervalMS() {
		return this.autoCommitIntervalMS;
	}

	public void setAutoCommitIntervalMS(Long autoCommitIntervalMS) {
		this.autoCommitIntervalMS = autoCommitIntervalMS;
	}

	public Integer getNumConsumerFetchers() {
		return this.numConsumerFetchers;
	}

	public void setNumConsumerFetchers(Integer numConsumerFetchers) {
		this.numConsumerFetchers = numConsumerFetchers;
	}

	public ConfigConstants.AutoOffsetResetFlag getAutoOffsetResetFlag() {
		return this.autoOffsetResetFlag;
	}

	public void setAutoOffsetResetFlag(ConfigConstants.AutoOffsetResetFlag autoOffsetResetFlag) {
		this.autoOffsetResetFlag = autoOffsetResetFlag;
	}

	public IKafkaClientsExecutorService getKafkaClientsExecutorService() {
		return this.kafkaClientsExecutorService;
	}

	public void setKafkaClientsExecutorService(IKafkaClientsExecutorService kafkaClientsExecutorService) {
		this.kafkaClientsExecutorService = kafkaClientsExecutorService;
	}

	public void setConsumerProps(Properties consumerProps) {
		this.consumerProps = consumerProps;
	}

	public Boolean getIsBatch() {
		return this.isBatch;
	}

	public Integer getBatchSize() {
		return this.batchSize;
	}

	public Boolean getCatchExceptionEnable() {
		return this.catchExceptionEnable;
	}

	public void setCatchExceptionEnable(Boolean catchExceptionEnable) {
		this.catchExceptionEnable = catchExceptionEnable;
	}
}