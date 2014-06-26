package com.pardot.rhombus.cobject.statement;

import java.util.Iterator;
import java.util.Map;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 4/13/13
 */
public interface CQLStatementIterator extends Iterator<CQLStatement>{

	public void setClientFilters(Map<String, Object> clientFilters);
	public Map<String, Object> getClientFilters();
	public boolean hasNext(long currentResultCount);
	public boolean isBounded();
	public long size();
	public void nextShard();
}
