package com.adou.example.utils.kafka.producer;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.utils.VerifiableProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adou.example.utils.kafka.core.config.ProducerCfg;

public class ProducerLib {
	private static final Map<String, List<Producer<?>>> PRODUCER_LIB = new HashMap();
	private static final Map<String, AtomicInteger> PRODUCER_INTEGER_LIB = new HashMap();
	private static final ThreadLocal<Map<String, Producer<?>>> SESSIONS = new ThreadLocal();

	private static final Logger LOG = LoggerFactory.getLogger(ProducerLib.class);

	private static synchronized void buildProducer(String id, ProducerCfg<?> producerCfg,
			VerifiableProperties verifiableProps) throws IllegalArgumentException, SecurityException,
			InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		if ((id == null) || (producerCfg == null)) {
			throw new NullPointerException("id and producerConfig is null");
		}
		LOG.info("build Producer with id:" + id + " and topic:" + producerCfg.getTopic());
		List producerList = new ArrayList();
		for (int i = 0; i < producerCfg.getPoolSize(); i++) {
			Producer producer = new Producer(producerCfg, verifiableProps);
			producerList.add(producer);
		}
		PRODUCER_LIB.put(id, producerList);
		AtomicInteger atomicInteger = new AtomicInteger(0);
		PRODUCER_INTEGER_LIB.put(id, atomicInteger);
	}

	private static Producer<?> nextProducer(String id) {
		int index = ((AtomicInteger) PRODUCER_INTEGER_LIB.get(id)).incrementAndGet();

		if (index >= ((List) PRODUCER_LIB.get(id)).size()) {
			((AtomicInteger) PRODUCER_INTEGER_LIB.get(id)).set(0);
			index = 0;
		}
		LOG.debug("return the producer with id:" + id + " and index:" + index);
		return (Producer) ((List) PRODUCER_LIB.get(id)).get(index);
	}

	private static Producer<?> getSessionProducer(String id) {
		Map sessionProducerLib = (Map) SESSIONS.get();
		if (SESSIONS.get() == null) {
			sessionProducerLib = new HashMap();
			SESSIONS.set(sessionProducerLib);
		}
		return (Producer) sessionProducerLib.get(id);
	}

	private static void putSessionProducer(String id, Producer<?> producer) {
		Map sessionProducerLib = (Map) SESSIONS.get();
		if (SESSIONS.get() == null) {
			sessionProducerLib = new HashMap();
			SESSIONS.set(sessionProducerLib);
		}
		sessionProducerLib.put(id, producer);
	}

	public static synchronized Producer<?> getOrCreateProducer(String id, ProducerCfg<?> producerCfg,
			VerifiableProperties verifiableProps) throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, IllegalArgumentException, SecurityException, InvocationTargetException,
			NoSuchMethodException {
		Producer producer = getSessionProducer(id);
		if (producer != null) {
			LOG.debug("Current session exist producer with id:" + id + ",using existed producer!");
			return producer;
		}
		if (PRODUCER_LIB.get(id) == null) {
			buildProducer(id, producerCfg, verifiableProps);
		}
		producer = nextProducer(id);
		putSessionProducer(id, producer);
		return producer;
	}

	public static Producer<?> getOrCreateProducer(String id, ProducerCfg<?> producerCfg) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, IllegalArgumentException, SecurityException,
			InvocationTargetException, NoSuchMethodException {
		return getOrCreateProducer(id, producerCfg, null);
	}
}
