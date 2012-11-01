/* 
 * RconnectionUtil.java
 * 
 * Created on 2011-7-13
 * 
 * Copyright(C) 2011, by 360buy.com.
 * 
 * Original Author: 刘杨
 * Contributor(s):
 * 
 * Changes 
 * -------
 * $Log$
 */
package com.jd.ipc.mapreduce.r.util;

import java.util.Properties;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;

/**
 * 
 * @author 刘杨
 * @version ${Revision}
 */
public class RConnectionUtil {
	private final static Logger log = Logger.getLogger(RConnectionUtil.class);
	private static GenericObjectPool pool = null;
	private static PoolableObjectFactory poFactory = null;
	static Properties properties = new Properties();

	/* 初始化 */
	static {
		int objectmaxnum = 1;
		int waitobjecttime = 1;
		int disposetime = 1000;
		int maxIdleNum = 1;
		int maxActiveNum = 1;
		int minIdleNum = 1;
		try {
			properties.load(RConnectionUtil.class.getResourceAsStream("/conf/r.properties"));
			objectmaxnum = Integer.parseInt(properties.getProperty("objectMaxNum"));
			waitobjecttime = Integer.parseInt(properties.getProperty("waitObjectTime"));
			disposetime = Integer.parseInt(properties.getProperty("disposeTime"));
			maxIdleNum = Integer.parseInt(properties.getProperty("maxIdleNum"));
			maxActiveNum = Integer.parseInt(properties.getProperty("maxActiveNum"));
			minIdleNum = Integer.parseInt(properties.getProperty("minIdleNum"));
		} catch (Exception e) {
			log.error("Init Rserve Error", e);
		}
		poFactory = new RConnectionFactory();
		pool = new GenericObjectPool(poFactory, objectmaxnum, GenericObjectPool.WHEN_EXHAUSTED_BLOCK, waitobjecttime, true, true);
		pool.setMaxActive(maxActiveNum);
		pool.setMinIdle(minIdleNum);
		pool.setTimeBetweenEvictionRunsMillis(disposetime);
		pool.setMaxIdle(maxIdleNum);
	}

	/**
	 * 
	 * method: 返回RConnection
	 * 
	 * @param rConnection
	 *            创建日期： 2011-7-13 Copyright(C) 2011, by 刘杨
	 */
	public static void returnRConnection(RConnection rConnection) {
		try {
			if (rConnection != null) {
				pool.returnObject(rConnection);
			}
		} catch (Exception e) {
			log.error("归还R连接失败", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * 
	 * method: 销毁RConnection
	 * 
	 * @param rConnection
	 *            RConnection 创建日期： 2011-7-13 Copyright(C) 2011, by liuyang
	 */
	public static void destroyRConnection(RConnection rConnection) {
		try {
			if (rConnection != null) {
				poFactory.destroyObject(rConnection);
			}
		} catch (Exception e) {
			log.error("销毁R连接失败", e);
			throw new RuntimeException(e);
		}
	}

	public static RConnection getRConnection() {
		try {
			Object obj = pool.borrowObject();
			return (RConnection) obj;
		} catch (Exception ex) {
			log.error("获取R连接失败", ex);
			throw new RuntimeException(ex);
		}

	}

	public static void closePool() {
		try {
			if (pool != null) {
				pool.close();
				pool = null;
			}
		} catch (Exception ex) {
			log.error("销毁对象池失败", ex);
			throw new RuntimeException(ex);
		}
	}

	public static Properties getProperties() {
		return properties;
	}

}
