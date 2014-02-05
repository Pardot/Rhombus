package com.pardot.rhombus.cobject.statement;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.pardot.rhombus.cobject.CObjectOrdering;
import com.pardot.rhombus.cobject.statement.CQLStatement;
import com.pardot.rhombus.cobject.statement.CQLStatementIterator;

import java.util.Iterator;
import java.util.List;


/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 4/13/13
 */
public class UnboundableCQLStatementIterator extends BaseCQLStatementIterator {


	private long limit = 0;
	private long numberRemaining = 0;
	private long size = 0;
	private CQLStatement CQLTemplate = null;
	private Range<Long> keyRange;
	private Iterator<Long> keyIterator = null;


	public UnboundableCQLStatementIterator(Range<Long> shardKeyList, long limit, CObjectOrdering ordering, CQLStatement CQLTemplate){
		this.keyRange = shardKeyList;
		ContiguousSet<Long> set = ContiguousSet.create(shardKeyList, DiscreteDomain.longs());
		this.keyIterator = (ordering == CObjectOrdering.ASCENDING) ? set.iterator() : set.descendingIterator();
		this.size = (long)set.size();
		this.limit = limit;
		this.numberRemaining = this.limit;
		this.CQLTemplate = CQLTemplate;
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
		//shardid is the first value and limit should be the last value
		values.add(0,this.keyIterator.next());
		CQLStatement ret = CQLStatement.make(String.format(CQLTemplate.getQuery(),numberRemaining),values.toArray());
		ret.setCacheable(CQLTemplate.isCacheable());
		return ret;
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

}
