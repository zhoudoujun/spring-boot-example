package com.adou.example.utils.kafka.core.config;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import kafka.utils.VerifiableProperties;

import com.adou.example.utils.kafka.consumer.handler.IConsumerHandler;
import com.adou.example.utils.kafka.core.ConfigConstants;
import com.adou.example.utils.kafka.core.executor.IKafkaClientsExecutorService;

public class ConsumerCfg<V extends Serializable> {
	private final Properties extProps;
	private final String id;
	private final String topic;
	private final Class<IConsumerHandler<V>> handlerClass;
	private final String groupID;
	private final String zookeeperConnect;
	private final Boolean autoCommitEnable;
	private final Long autoCommitIntervalMS;
	private final Integer numConsumerFetchers;
	private final ConfigConstants.AutoOffsetResetFlag autoOffsetResetFlag;
	private final Integer batchSize;
	private Boolean isBatch = Boolean.valueOf(false);

	private Boolean catchExceptionEnable = Boolean.valueOf(true);
	private final Class<IKafkaClientsExecutorService> kafkaClientsExecutorServiceClass;

	public ConsumerCfg(Properties extProps, String id, String topic, Class<IConsumerHandler<V>> handlerClass,
			String groupID, String zookeeperConnect, Boolean autoCommitEnable, Boolean catchExceptionEnable,
			Boolean isBatch, Integer batchSize, Long autoCommitIntervalMS, Integer numConsumerFetchers,
			ConfigConstants.AutoOffsetResetFlag autoOffsetResetFlag,
			Class<IKafkaClientsExecutorService> kafkaClientsExecutorServiceClass) {
		this.extProps = extProps;
		this.id = id;
		this.topic = topic;
		this.handlerClass = handlerClass;
		this.groupID = groupID;
		this.zookeeperConnect = zookeeperConnect;
		this.autoCommitEnable = autoCommitEnable;
		this.catchExceptionEnable = catchExceptionEnable;
		this.isBatch = isBatch;
		this.batchSize = batchSize;
		this.autoCommitIntervalMS = autoCommitIntervalMS;
		this.numConsumerFetchers = numConsumerFetchers;
		this.autoOffsetResetFlag = autoOffsetResetFlag;
		this.kafkaClientsExecutorServiceClass = kafkaClientsExecutorServiceClass;
	}

	public Properties getExtProps() {
		return (Properties) this.extProps.clone();
	}

	public String getId() {
		return this.id;
	}

	public String getTopic() {
		return this.topic;
	}

	public Class<IConsumerHandler<V>> getHandlerClass() {
		return this.handlerClass;
	}

	public String getGroupID() {
		return this.groupID;
	}

	public String getZookeeperConnect() {
		return this.zookeeperConnect;
	}

	public Boolean getAutoCommitEnable() {
		return this.autoCommitEnable;
	}

	public Long getAutoCommitIntervalMS() {
		return this.autoCommitIntervalMS;
	}

	public Integer getNumConsumerFetchers() {
		return this.numConsumerFetchers;
	}

	public ConfigConstants.AutoOffsetResetFlag getAutoOffsetResetFlag() {
		return this.autoOffsetResetFlag;
	}

	public Class<IKafkaClientsExecutorService> getKafkaClientsExecutorServiceClass() {
		return this.kafkaClientsExecutorServiceClass;
	}

	public Integer getBatchSize() {
		return this.batchSize;
	}

	public Boolean getIsBatch() {
		return this.isBatch;
	}

	public Boolean getCatchExceptionEnable() {
		return this.catchExceptionEnable;
	}

	public IConsumerHandler<V> getHandlerInstance(VerifiableProperties verifiableProps) throws InstantiationException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Constructor[] constructors = this.handlerClass.getConstructors();
		Constructor noParamConstructor = null;
		for (Constructor constructor : constructors) {
			Class[] paramTypes = constructor.getParameterTypes();
			if ((paramTypes != null) && (paramTypes.length == 1)) {
				if (paramTypes[0].equals(VerifiableProperties.class)) {
					return (IConsumerHandler) constructor.newInstance(new Object[] { verifiableProps });
				}
			}
			if ((paramTypes == null) || (paramTypes.length == 0)) {
				noParamConstructor = constructor;
			}
		}
		if (noParamConstructor != null) {
			return (IConsumerHandler) noParamConstructor.newInstance(new Object[0]);
		}
		throw new NullPointerException(
				"IConsumerHandler must have a constructor with eithor no params or one param kafka.utils.VerifiableProperties");
	}
}
