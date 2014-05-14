package com.pardot.rhombus.cobject.statement;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;
import com.pardot.rhombus.cobject.CQLExecutor;

import java.util.*;

/**
 * Created by himanshu.pagey on 5/12/14.
 */
public class CQLExecutorIterator implements Iterator {

	private List<Row> page;
	private	int pageSize = 50;
	private int nextItem = 0;
	private int currentPage = 0;
	private int currentShardId = 1;
	private CQLExecutor cqlExecutor;
	private BaseCQLStatementIterator statementIterator;
	boolean hasMore = false;
	public int statementNumber = 0;

	public CQLExecutorIterator(CQLExecutor cqlExecutor, BaseCQLStatementIterator statementIterator){

		this.cqlExecutor = cqlExecutor;
		this.statementIterator = statementIterator;
	}

	public boolean hasNext(){
		fetchIfNeeded();

		if (page.size() > nextItem){
			return true;
		} else if (statementIterator.hasNext()){

			//we are rolling over to next shard
			statementIterator.nextShard();
			currentPage = 0;
			nextItem = 0;
			fetchIfNeeded();
			return true;

		} else {
			return false;
		}
	}

	public Row next(){

		fetchIfNeeded();
		Row ret = null;

		if (page.size() > nextItem){

			// we are exhausting the items in the current page
			ret = page.get(nextItem);
			nextItem++;

		} else if(statementIterator.hasNext()) {

			//we are rolling over to next shard
			statementIterator.nextShard();
			currentPage = 0;
			nextItem = 0;
			fetchIfNeeded();
			ret = page.get(nextItem);
		}

		return ret;
	}

	private void fetchIfNeeded(){

		if (currentPage == 0){

			statementIterator.setLimit(pageSize+1);
			CQLStatement cql = statementIterator.next();
			ResultSet resultSet = cqlExecutor.executeSync(cql);
			statementNumber++;
			populatePage(resultSet);

			if (page.size() > pageSize) {
				hasMore = true;
			} else {
				hasMore = false;
			}
			currentPage++;

		} else if (hasMore && (nextItem == pageSize)){

			Row row = page.get(nextItem);
			UUID uuid = row.getUUID("id");
			statementIterator.setNextUuid(uuid);
			CQLStatement cql = statementIterator.next();
			ResultSet resultSet = cqlExecutor.executeSync(cql);
			statementNumber++;

			populatePage(resultSet);

			if (page.size() > pageSize) {
				hasMore = true;
			} else {
				hasMore = false;
			}
			nextItem = 0;
			currentPage++;
		}
	}

	private void populatePage(ResultSet resultSet){

		page = Lists.newArrayList();
		for(Row row : resultSet) {
			page.add(row);
		}

	}

	public void remove(){

		statementIterator.remove();
	}

	public Map<String,Object> getClientFilters(){
		return statementIterator.getClientFilters();
	}

	public void setClientFilters(Map<String, Object> clientFilters){

		statementIterator.setClientFilters(clientFilters);
	}

	public void setPageSize(int pageSize){

		this.pageSize = pageSize;
	}

	public int getPageSize(){
		return pageSize;
	}
}
