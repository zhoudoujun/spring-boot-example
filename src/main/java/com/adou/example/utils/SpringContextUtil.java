package com.adou.example.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;

/**
 * Spring上下文对象的管理工具类。<br/>
 * 请确保在容器加载完了后调用setApplicationContext方法。<br/>
 * Date: 2019年7月17日16:24:04<br/>
 * @author: zhoudoujun01
 */
public class SpringContextUtil {

    /**
     * 上下文对象实例
     */
    private static ApplicationContext applicationContext;

    public static void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        if (SpringContextUtil.applicationContext == null) {
            SpringContextUtil.applicationContext = applicationContext;
        }
    }

    /**
     * 获取applicationContext
     * 
     * @return 上下文对象实例
     */
    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    /**
     * 通过实例名获取实例对象 .
     * 
     * @param name 实例名
     * @return 实例对象
     */
    public static Object getBean(String name) {
        return getApplicationContext().getBean(name);
    }

    /**
     * 通过类获取实例对象.
     * 
     * @param clazz 类
     * @return 实例对象
     */
    public static <T> T getBean(Class<T> clazz) {
        return getApplicationContext().getBean(clazz);
    }

    /**
     * 通过实例名,以及类返回指定的实例对象
     * 
     * @param name 实例名
     * @param clazz 类
     * @return 实例对象
     */
    public static <T> T getBean(String name, Class<T> clazz) {
        return getApplicationContext().getBean(name, clazz);
    }

}