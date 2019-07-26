package com.adou.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import com.adou.example.utils.SpringContextUtil;
import com.adou.example.utils.kafka.consumer.Consumer;
import com.adou.example.utils.kafka.core.config.KafkaClientsConfig;

/**
 * @Description:容器启动完毕之后,加载kafka
 * 
 * @author zhoudoujun01
 * @date:2019年4月24日 下午4:45:08
 */
@Component
public class AfterStartup implements ApplicationListener<ContextRefreshedEvent> {
	private static final Logger LOG = LoggerFactory.getLogger(AfterStartup.class);
    /**
     * 加载kafka
     */
    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        SpringContextUtil.setApplicationContext(contextRefreshedEvent.getApplicationContext());
        startKafkaCustomer();
    }
    
	@SuppressWarnings("unchecked")
	private void startKafkaCustomer() {
		try {
			LOG.info("-------------开启kafka消息监听-------------");
			KafkaClientsConfig config = new KafkaClientsConfig("kafka-clients-cfg.xml");
			//
			createConcumer(config, "KAFKA_CLIENTS_CFG_SPRING_BOOT_EXAMPLE");
			LOG.info("--------------启动kafka监听成功--------------");
		} catch (Exception e) {
			LOG.error("--------------启动kafka监听失败--------------", e);
		}
	}
	
    
    private void createConcumer(KafkaClientsConfig config, String name) {
		try {
			Consumer<?> consumer = config.newConsumer(name);
			consumer.startReceive();
			LOG.info("------------------ createConcumer :consumerName="+ name + "--------------------");
		} catch (Exception e) {
			LOG.error("------------------ createConcumer Error:consumerName="+ name + "----------------------", e);
		}
	}
}