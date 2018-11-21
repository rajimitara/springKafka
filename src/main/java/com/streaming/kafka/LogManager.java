package com.streaming.kafka;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogManager {

final Logger logger;
	
	public LogManager(Object obj) {
		this.logger = LoggerFactory.getLogger(obj.getClass());
	}

	public void logInfo(String logMsg) {
		logger.info(logMsg);
	}
	
	public void logInfo(String threadName, String logMsg) {
		logger.info("ThreadName=" + threadName + " - " + logMsg);
	}
	
	public void logInfo(String triageId, String runId, String logMsg) {
		logger.info(triageId + "-" + runId + "-" + logMsg);
	}

	public void logDebug(String logMsg) {
		logger.debug(logMsg);
	}
	
	public void logDebug(String threadName, String logMsg) {
		logger.debug("ThreadName=" + threadName + " - " + logMsg);
	}
	
	public void logDebug(long triageId, long runId, String logMsg) {
		logger.debug(triageId + "-" + runId + "-" + logMsg);
	}

	public void logError(String logMsg) {
		logger.error(logMsg);
	}
	
	public void logError(String threadName, String logMsg) {
		logger.error("ThreadName=" + threadName + " - " + logMsg);
	}
	
	public void logError(String threadName, String logMsg, Exception ex) {
		logger.error("ThreadName=" + threadName + " - " + logMsg + " - Exception: " + ex.getMessage() + " - Stacktrace: " + Arrays.toString(ex.getStackTrace()));
	}
	
	public void logError(long triageId, long runId, String logMsg) {
		logger.error(triageId + "-" + runId + "-" + logMsg);
	}
	
	public void logError(long triageId, long runId, String logMsg, Exception ex) {
		logger.error(triageId + "-" + runId + "-" + logMsg + " - Exception: " + ex.getMessage() + " - Stacktrace: " + Arrays.toString(ex.getStackTrace()));
	}

	public void logWarning(String logMsg) {
		logger.warn(logMsg);
	}
	
	public void logWarning(String threadName, String logMsg) {
		logger.warn("ThreadName=" + threadName + " - " + logMsg);
	}
	
	public void logWarning(long triageId, long runId, String logMsg) {
		logger.warn(triageId + "-" + runId + "-" + logMsg);
	}

}
