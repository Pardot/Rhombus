package com.pardot.rhombus.cobject.statement;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.pardot.rhombus.cobject.CObjectOrdering;

import java.util.Iterator;
import java.util.List;


/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 4/13/13
 */
public class UnboundableCQLStatementIterator extends BaseCQLStatementIterator {


	private long numberRemaining = 0;
	private long size = 0;
	private CQLStatement CQLTemplate = null;
	private Range<Long> keyRange;
	private Iterator<Long> keyIterator = null;
	private String objectName;

	public UnboundableCQLStatementIterator(Range<Long> shardKeyList, long limit, CObjectOrdering ordering, CQLStatement CQLTemplate, String objectName){
		this.keyRange = shardKeyList;
		ContiguousSet<Long> set = ContiguousSet.create(shardKeyList, DiscreteDomain.longs());
		this.keyIterator = (ordering == CObjectOrdering.ASCENDING) ? set.iterator() : set.descendingIterator();
		this.ordering = ordering;
		this.size = (long)set.size();
		this.limit = limit;
		this.numberRemaining = this.limit;
		this.CQLTemplate = CQLTemplate;
		this.setObjectName(objectName);

	}

	@Override
	public boolean hasNext() {
		return keyIterator.hasNext();
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
		List values = Lists.newArrayList(CQLTemplate.getValues());
		String query = CQLTemplate.getQuery();
		//shardid is the first value
		if (currentShardId == 0){
			nextShard();
		}
		values.add(0, currentShardId);

		// the id in the where clause is the last value
		if (nextUuid != null){

			setStartEndUuidIndexes(CQLTemplate, values.toArray());

			if (ordering == CObjectOrdering.ASCENDING){

				if (startUUidIndex == 0){

					query = insertStartUuidClause(CQLTemplate.getQuery());
					values.add(endUuidIndex, nextUuid);

				} else {
					values.set(startUUidIndex, nextUuid);
				}

			} else {
				values.set(endUuidIndex, nextUuid);
			}

		}

		// numberRemaining is the limit
		CQLStatement ret = CQLStatement.make(String.format(query, numberRemaining), this.getObjectName(), values.toArray());
		return ret;
	}

	@Override
	public void setLimit(int limit){
		this.numberRemaining = limit;
	}

	@Override
	public void nextShard(){

		currentShardId = keyIterator.next();
	}

	public boolean isBounded(){
		return (keyRange.hasLowerBound() && keyRange.hasUpperBound());
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
