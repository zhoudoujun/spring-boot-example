package com.adou.example.utils.kafka.core.executor;

public abstract interface IKafkaClientsExecutorService {
	public abstract void execute(Runnable paramRunnable);

	public abstract void shutdown();

	public abstract void reload();
}
