package com.roncoo.eshop.inventory.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.roncoo.eshop.inventory.thread.RequestProcessorThreadPool;

/**
 * 系统初始化监听器	
 * @author alonzo
 *
 */
public class InitListener implements ServletContextListener {

	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		// 初始化工作线程池和内存队列
		RequestProcessorThreadPool.init();
	}

	@Override
	public void contextInitialized(ServletContextEvent arg0) {

	}

}
