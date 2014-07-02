package com.pardot.rhombus.cobject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * User: Mike Frank
 * Date: 6/30/14
 */
public abstract class CObjectTokenVisitor {

	private static Logger logger = LoggerFactory.getLogger(CObjectTokenVisitor.class);

	private long tokensVisited = 0;
	private long tokenLogInterval = 1000;
	private String idField = "id";
	private String name;

	public abstract void applyAction(Map<String, Object> object);

	public abstract boolean shouldInclude(Map<String, Object> object);

	public CObjectTokenVisitor(String name) {
		this.name = name;
	}

	public void visit(Map<String, Object> object) {
		tokensVisited++;
		Object id = object.get(idField);
		if(tokensVisited % tokenLogInterval == 0) {
			logger.info("Visitor {} visiting token {} with id {}", name, tokensVisited, id);
		}
		if(shouldInclude(object)) {
			applyAction(object);
		}
	}

	public long getTokenLogInterval() {
		return tokenLogInterval;
	}

	public void setTokenLogInterval(long tokenLogInterval) {
		this.tokenLogInterval = tokenLogInterval;
	}

	public String getIdField() {
		return idField;
	}

	public void setIdField(String idField) {
		this.idField = idField;
	}

	public long getTokensVisited() {
		return this.tokensVisited;
	}
}
