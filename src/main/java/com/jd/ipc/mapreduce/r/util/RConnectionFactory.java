/* 
 * RconnectionFactory.java
 * 
 * Created on 2011-7-13
 * 
 * Copyright(C) 2011, by 360buy.com.
 * 
 * Original Author: yourname123
 * Contributor(s):
 * 
 * Changes 
 * -------
 * $Log$
 */
package com.jd.ipc.mapreduce.r.util;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;

public class RConnectionFactory implements PoolableObjectFactory {
	private final static Logger log = Logger.getLogger(RConnectionFactory.class);
	private static String RSERVE_IP = "127.0.0.1";
	static {
		try {
			RSERVE_IP = RConnectionUtil.getProperties().getProperty("RServeIP");
			log.debug("R 连接地址为:" + RSERVE_IP);
		} catch (Exception e) {
			log.error("Init Rserve IP error", e);
		}
	}

	public void activateObject(Object arg0) throws Exception {
		log.debug("enter method:activateObject");
	}

	public void destroyObject(Object arg0) throws Exception {
		RConnection rConnection = (RConnection) arg0;
		rConnection.close();
		rConnection = null;
	}

	public Object makeObject() throws Exception {
		RConnection rConnection = new RConnection(RSERVE_IP);
		log.debug("R ip is:" + RSERVE_IP);
		// rConnection.eval("library(\"TSA\")");;
		return rConnection;
	}

	public void passivateObject(Object arg0) throws Exception {
		log.debug("enter method:passivateObject");

	}

	public boolean validateObject(Object arg0) {
		return true;
	}

}
