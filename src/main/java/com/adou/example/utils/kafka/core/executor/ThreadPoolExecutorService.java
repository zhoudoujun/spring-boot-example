package com.adou.example.utils.kafka.core.executor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadPoolExecutorService implements IKafkaClientsExecutorService {
	private static final Logger LOG = LoggerFactory.getLogger(ThreadPoolExecutorService.class);

	private ExecutorService executors = Executors.newCachedThreadPool();

	public void execute(Runnable runnable) {
		this.executors.submit(runnable);
	}

	public void shutdown() {
		if (this.executors != null)
			this.executors.shutdown();
		try {
			if (!this.executors.awaitTermination(5000L, TimeUnit.MILLISECONDS))
				LOG.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
		} catch (InterruptedException e) {
			LOG.info("Interrupted during shutdown, exiting uncleanly");
		}
	}

	public void reload() {
		if ((this.executors == null) || (this.executors.isShutdown()))
			this.executors = Executors.newCachedThreadPool();
	}
}
