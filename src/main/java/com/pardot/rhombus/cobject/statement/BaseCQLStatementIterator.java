package com.pardot.rhombus.cobject.statement;

import java.util.Map;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 2/4/14
 */
public abstract class BaseCQLStatementIterator implements CQLStatementIterator {
	private Map<String, Object> clientFilters;

	public Map<String, Object> getClientFilters() {
		return clientFilters;
	}

	public void setClientFilters(Map<String, Object> clientFilters) {
		this.clientFilters = clientFilters;
	}
}
