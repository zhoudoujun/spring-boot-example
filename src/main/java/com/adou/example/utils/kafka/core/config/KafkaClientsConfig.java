package com.adou.example.utils.kafka.core.config;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.utils.VerifiableProperties;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adou.example.utils.kafka.consumer.Consumer;
import com.adou.example.utils.kafka.consumer.handler.IConsumerHandler;
import com.adou.example.utils.kafka.core.ConfigConstants;
import com.adou.example.utils.kafka.core.exception.IDNotUniqueException;
import com.adou.example.utils.kafka.core.executor.IKafkaClientsExecutorService;
import com.adou.example.utils.kafka.producer.Producer;
import com.adou.example.utils.kafka.producer.ProducerLib;
import com.adou.example.utils.kafka.producer.handler.AbstractProducerHandler;

public class KafkaClientsConfig {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaClientsConfig.class);
	public static final String DEFAILT_CFG_XML_FILE = "kafka-clients-cfg.xml";
	private Document cfgXmlDocument;
	private VerifiableProperties verfProps;
	private Map<String, ConsumerCfg<?>> consumers;
	private Map<String, ProducerCfg<?>> producers;
	public static KafkaClientsConfig DEFAULT_INSTANCE;
	private static final String CONSUMER_XPATH = "//kafka-clients/consumer";
	private static final String PRODUCER_XPATH = "//kafka-clients/producer";

	public KafkaClientsConfig(File cfgXmlFile) throws DocumentException, InstantiationException,
			IllegalAccessException, ClassNotFoundException, IllegalArgumentException, SecurityException,
			InvocationTargetException, NoSuchMethodException {
		SAXReader saxReader = new SAXReader();
		this.cfgXmlDocument = saxReader.read(cfgXmlFile);
		reloadProducers();
		reloadConsumers();
	}

	public KafkaClientsConfig(String cfgXmlFilePath) throws DocumentException, InstantiationException,
			IllegalAccessException, ClassNotFoundException, IllegalArgumentException, SecurityException,
			InvocationTargetException, NoSuchMethodException {
		SAXReader saxReader = new SAXReader();

		InputStream inputStream = KafkaClientsConfig.class.getResourceAsStream("/" + cfgXmlFilePath);

		this.cfgXmlDocument = saxReader.read(inputStream);
		reloadProducers();
		reloadConsumers();
	}

	public KafkaClientsConfig(InputStream inputStream) throws DocumentException, InstantiationException,
			IllegalAccessException, ClassNotFoundException, IllegalArgumentException, SecurityException,
			InvocationTargetException, NoSuchMethodException {
		SAXReader saxReader = new SAXReader();
		this.cfgXmlDocument = saxReader.read(inputStream);
		reloadProducers();
		reloadConsumers();
	}

	private KafkaClientsConfig() throws DocumentException, InstantiationException, IllegalAccessException,
			ClassNotFoundException, IllegalArgumentException, SecurityException, InvocationTargetException,
			NoSuchMethodException {
		this("kafka-clients-cfg.xml");
	}

	public void reloadProducers() throws InstantiationException, IllegalAccessException, ClassNotFoundException,
			IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException {
		LOG.info("Reload Producers Begin...");
		this.producers = new HashMap();

		List<Element> producersNodes = this.cfgXmlDocument.selectNodes("//kafka-clients/producer");
		if ((producersNodes == null) || (producersNodes.size() == 0)) {
			LOG.info("No Producer...");
			return;
		}
		for (Element producerNode : producersNodes) {
			Properties producerProps = new Properties();
			String id = null;
			String topic = null;
			int poolSize = -1;
			boolean asyncEnable = false;
			Class handlerClass = null;
			String kafkaBrokes = null;
			ConfigConstants.RequestRequiredAcksFlag requestRequiredAcks = null;
			LOG.debug("==============================================");
			Iterator ite = producerNode.elementIterator();
			while (ite.hasNext()) {
				Element ele = (Element) ite.next();
				String eleName = ele.getName();
				String eleText = ele.getText();
				if (eleName != null) {
					LOG.debug(eleName + ":" + eleText);

					if (ConfigConstants.ProducersKey.EXT_PROPS.getKey().equals(eleName)) {
						Iterator extPropsIte = ele.elementIterator();
						while (extPropsIte.hasNext()) {
							Element propsEle = (Element) extPropsIte.next();
							LOG.debug(propsEle.getName() + ":" + propsEle.getText());
							if ((propsEle.getText() != null) && (!"".equals(propsEle.getText().trim()))) {
								producerProps.put(propsEle.getName(), propsEle.getText());
							}
						}
					}
					if ((eleText != null) && (!"".equals(eleText.trim()))) {
						if (ConfigConstants.ProducersKey.ID.getKey().equals(eleName)) {
							id = eleText;
						} else if (ConfigConstants.ProducersKey.TOPIC.getKey().equals(eleName)) {
							topic = eleText;
						} else if (ConfigConstants.ProducersKey.HANDLER_CLASS.getKey().equals(eleName)) {
							Class cls = Class.forName(eleText);
							if (AbstractProducerHandler.class.isAssignableFrom(cls))
								handlerClass = cls;
							else
								throw new RuntimeException("handler.class is not extends AbstractProducerHandler");
						} else if (ConfigConstants.ProducersKey.KAFKA_BROKER_LIST.getKey().equals(eleName)) {
							kafkaBrokes = eleText;
						} else if (ConfigConstants.ProducersKey.REQUEST_REQUIRED_ACKS.getKey().equals(eleName)) {
							Integer requestRequiredAcksInt = Integer.valueOf(Integer.parseInt(eleText));
							requestRequiredAcks = ConfigConstants.RequestRequiredAcksFlag
									.getFlag(requestRequiredAcksInt);
						} else if (ConfigConstants.ProducersKey.POOL_SIZE.getKey().equals(eleName)) {
							Integer producerPoolSize = Integer.valueOf(Integer.parseInt(eleText));
							if ((producerPoolSize != null) && (producerPoolSize.intValue() > 0))
								poolSize = producerPoolSize.intValue();
						} else if (ConfigConstants.ProducersKey.ASYNC_ENABLE.getKey().equals(eleName)) {
							asyncEnable = Boolean.parseBoolean(eleText);
						}
					}
				}
			}
			if ((id != null) && (this.producers.containsKey(id))) {
				throw new IDNotUniqueException("Mutiple producers with id:" + id);
			}
			LOG.debug("==============================================");
			this.producers.put(id, new ProducerCfg(producerProps, id, topic, poolSize, asyncEnable, handlerClass,
					kafkaBrokes, requestRequiredAcks));
		}

		LOG.info("Reload Producers End.");
	}

	public void reloadConsumers() throws InstantiationException, IllegalAccessException, ClassNotFoundException,
			IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException {
		LOG.info("Reload Consumers Begin...");
		this.consumers = new HashMap();

		List<Element> consumersNodes = this.cfgXmlDocument.selectNodes("//kafka-clients/consumer");
		if ((consumersNodes == null) || (consumersNodes.size() == 0)) {
			LOG.info("No Consumer...");
			return;
		}
		for (Element consumerNode : consumersNodes) {
			Properties prop = new Properties();
			String id = null;
			String topic = null;
			Class handlerCls = null;
			String groupID = null;
			String zookeeperConnect = null;
			Boolean autoCommit = Boolean.valueOf(true);
			Long autoCommitIntervalMS = null;
			Integer numConsumerFetchers = null;
			ConfigConstants.AutoOffsetResetFlag autoOffsetResetFlag = null;
			Class kafkaExecutorServiceCls = null;
			Boolean isBatch = Boolean.valueOf(false);
			Integer batchSize = Integer.valueOf(50);
			Boolean catchExceptionEnable = Boolean.valueOf(true);

			LOG.debug("==============================================");
			Iterator ite = consumerNode.elementIterator();
			while (ite.hasNext()) {
				Element ele = (Element) ite.next();
				String eleName = ele.getName();
				String eleText = ele.getText();
				if (eleName != null) {
					LOG.debug(eleName + ":" + eleText);
					if (ConfigConstants.ConsumersKey.EXT_PROPS.getKey().equals(eleName)) {
						Iterator extPropsIte = ele.elementIterator();
						while (extPropsIte.hasNext()) {
							Element propEle = (Element) extPropsIte.next();
							if ((propEle.getText() != null) && (!"".equals(propEle.getText().trim()))) {
								prop.put(propEle.getName(), propEle.getText());
								LOG.debug(propEle.getName() + ":" + propEle.getText());
							}
						}
					} else if (ConfigConstants.ConsumersKey.EXECUTOR_SERVICE_CLASS.getKey().equals(eleName)) {
						Class cls = Class.forName(eleText);
						if (IKafkaClientsExecutorService.class.isAssignableFrom(cls))
							kafkaExecutorServiceCls = cls;
						else
							throw new RuntimeException(
									"executor.service.class is not extends IKafkaClientsExecutorService");
					} else if (ConfigConstants.ConsumersKey.ID.getKey().equals(eleName)) {
						id = eleText;
					} else if (ConfigConstants.ConsumersKey.TOPIC.getKey().equals(eleName)) {
						topic = eleText;
					} else if (ConfigConstants.ConsumersKey.HANDLER_CLASS.getKey().equals(eleName)) {
						Class cls = Class.forName(eleText);
						if (IConsumerHandler.class.isAssignableFrom(cls))
							handlerCls = cls;
						else
							throw new RuntimeException("handler.class is not extends AbstractConsumerHandler");
					} else if (ConfigConstants.ConsumersKey.GROUP_ID.getKey().equals(eleName)) {
						groupID = eleText;
					} else if (ConfigConstants.ConsumersKey.ZOOKEEPER_CONNECT.getKey().equals(eleName)) {
						zookeeperConnect = eleText;
					} else if ((ConfigConstants.ConsumersKey.AUTO_COMMIT_ENABLE.getKey().equals(eleName))
							&& (eleText != null) && (!"".equals(eleText.trim()))) {
						autoCommit = Boolean.valueOf(Boolean.parseBoolean(eleText));
					} else if ((ConfigConstants.ConsumersKey.CATCH_EXCEPTION_ENABLE.getKey().equals(eleName))
							&& (eleText != null) && (!"".equals(eleText.trim()))) {
						catchExceptionEnable = Boolean.valueOf(Boolean.parseBoolean(eleText));
					} else if ((ConfigConstants.ConsumersKey.IS_BATCH.getKey().equals(eleName)) && (eleText != null)
							&& (!"".equals(eleText.trim()))) {
						isBatch = Boolean.valueOf(Boolean.parseBoolean(eleText));
					} else if (ConfigConstants.ConsumersKey.BATCH_SIZE.getKey().equals(eleName)) {
						batchSize = Integer.valueOf(Integer.parseInt(eleText));
					} else if (ConfigConstants.ConsumersKey.AUTO_COMMIT_INTERVAL_MS.getKey().equals(eleName)) {
						autoCommitIntervalMS = Long.valueOf(Long.parseLong(eleText));
					} else if (ConfigConstants.ConsumersKey.NUM_CONSUMER_FETCHERS.getKey().equals(eleName)) {
						numConsumerFetchers = Integer.valueOf(Integer.parseInt(eleText));
					} else if (ConfigConstants.ConsumersKey.AUTO_OFFSET_RESET.getKey().equals(eleName)) {
						autoOffsetResetFlag = ConfigConstants.AutoOffsetResetFlag.getFlag(eleText);
						if (autoOffsetResetFlag == null)
							throw new NullPointerException("the auto.offset.reset must in {smallest,largest}");
					}
				}
			}
			if ((id != null) && (this.consumers.containsKey(id))) {
				throw new IDNotUniqueException("Mutiple consumers with id:" + id);
			}

			LOG.debug("==============================================");
			this.consumers.put(id, new ConsumerCfg(prop, id, topic, handlerCls, groupID, zookeeperConnect, autoCommit,
					catchExceptionEnable, isBatch, batchSize, autoCommitIntervalMS, numConsumerFetchers,
					autoOffsetResetFlag, kafkaExecutorServiceCls));
		}

		LOG.info("Reload Consumers End.");
	}

	public Consumer<?> newConsumer(String id, VerifiableProperties verifiableProps) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, IllegalArgumentException, SecurityException,
			InvocationTargetException, NoSuchMethodException {
		if (this.consumers == null) {
			reloadConsumers();
		}
		ConsumerCfg consumerCfg = (ConsumerCfg) this.consumers.get(id);
		return new Consumer(consumerCfg, verifiableProps);
	}

	public Consumer<?> newConsumer(String id) throws IllegalArgumentException, SecurityException,
			InstantiationException, IllegalAccessException, ClassNotFoundException, InvocationTargetException,
			NoSuchMethodException {
		return newConsumer(id, null);
	}

	public Producer<?> getOrCreateProducer(String id, VerifiableProperties verifiableProps)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, IllegalArgumentException,
			SecurityException, InvocationTargetException, NoSuchMethodException {
		if (this.producers == null) {
			reloadProducers();
		}
		ProducerCfg producerCfg = (ProducerCfg) this.producers.get(id);
		return ProducerLib.getOrCreateProducer(id, producerCfg, verifiableProps);
	}

	public Producer<?> getOrCreateProducer(String id) throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, IllegalArgumentException, SecurityException, InvocationTargetException,
			NoSuchMethodException {
		return getOrCreateProducer(id, null);
	}

	public Map<String, ConsumerCfg<?>> getConsumers() {
		return this.consumers;
	}

	public Map<String, ProducerCfg<?>> getProducers() {
		return this.producers;
	}

	static {
		try {
			DEFAULT_INSTANCE = new KafkaClientsConfig();
		} catch (Exception e) {
			LOG.warn("No default file:kafka-clients-cfg.xml");
		}
	}
}
