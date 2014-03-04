package com.pardot.rhombus.cobject.statement;

import com.google.common.collect.Lists;
import com.pardot.rhombus.cobject.statement.CQLStatement;
import com.pardot.rhombus.cobject.statement.CQLStatementIterator;

import java.util.Iterator;
import java.util.List;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 4/17/13
 */
public class BoundedLazyCQLStatementIterator extends BaseCQLStatementIterator {

	private long limit = 0;
	private long numberRemaining = 0;
	private long size = 0;
	private CQLStatement CQLTemplate = null;
	private Iterator<Long> shardIdIterator;
	private String objectName;

	public BoundedLazyCQLStatementIterator(List<Long> shardIds, CQLStatement CQLTemplate, long limit, String objectName){
		this.size = (long)shardIds.size();
		this.limit = limit;
		this.numberRemaining = this.limit;
		this.CQLTemplate = CQLTemplate;
		this.shardIdIterator = shardIds.iterator();
		this.setObjectName(objectName);
	}

	@Override
	public boolean hasNext() {
		return shardIdIterator.hasNext();
	}

	public boolean hasNext(long currentResultCount){
		numberRemaining = limit - currentResultCount;
		if( (this.limit != 0) && (currentResultCount >= this.limit) ){
			return false;
		}
		return this.hasNext();
	}

	@Override
	public CQLStatement next() {
		CQLStatement ret = CQLStatement.make(String.format(CQLTemplate.getQuery(), numberRemaining), this.getObjectName());
		List values = Lists.newArrayList(CQLTemplate.getValues());
		//shardid is the first value and limit should be the last value
		values.add(0,this.shardIdIterator.next());
		ret.setValues(values.toArray());
		return ret;
	}

	public boolean isBounded(){
		return true;
	}

	public long size(){
		return size;
	}

	@Override
	public void remove() {
		//To change body of implemented methods use File | Settings | File Templates.
	}

	public String getObjectName() {
		return objectName;
	}

	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}
}