package com.pardot.rhombus.cobject;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Iterator;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 4/15/13
 */
public class BoundedCQLStatementIterator implements CQLStatementIterator {

	private long size = 0;
	private Iterator<CQLStatement> statementIterator;
	private List<CQLStatement> cqlStatements;

	public BoundedCQLStatementIterator(List<CQLStatement> CQLStatements){
		this.size = (long)CQLStatements.size();
		this.cqlStatements = CQLStatements;
		this.statementIterator = CQLStatements.iterator();
	}

	public boolean allStatementsPreparable() {
		for(CQLStatement cqlStatement : cqlStatements) {
			if(!cqlStatement.isPreparable()) {
				return false;
			}
		}
		return true;
	}

	public static BoundedCQLStatementIterator condenseIterators(List<CQLStatementIterator> statementIterators) throws CQLGenerationException {
		List<CQLStatement> ret = Lists.newArrayList();
		for(CQLStatementIterator it: statementIterators){
			if(it.isBounded()){
				while(it.hasNext()){
					ret.add(it.next());
				}
			}
			else{
				throw new CQLGenerationException("Cannot Condense Unbounded Statement Iterators");
			}
		}
		return new BoundedCQLStatementIterator(ret);
	}

	@Override
	public boolean hasNext() {
		return statementIterator.hasNext();
	}

	public boolean hasNext(long currentResultCount){
		return this.hasNext();
	}

	@Override
	public CQLStatement next() {
		return statementIterator.next();
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


}
