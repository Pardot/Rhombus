package com.pardot.rhombus.cobject.statement;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;
import com.pardot.rhombus.cobject.CQLExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by himanshu.pagey on 5/12/14.
 */
public class CQLExecutorIterator implements Iterator {

	private static Logger logger = LoggerFactory.getLogger(CQLExecutorIterator.class);

	private List<Row> page;
	private	long pageSize = 50l;
	private int nextItem = 0;
	private CQLExecutor cqlExecutor;
	private CQLStatementIterator statementIterator;
	boolean moreResultsInShard = false;
	public int statementNumber = 0;

	public CQLExecutorIterator(CQLExecutor cqlExecutor, CQLStatementIterator statementIterator){
		this.cqlExecutor = cqlExecutor;
		this.statementIterator = statementIterator;
	}

	/**
	 * An ExecutorIterator has next if
	 * 1.  Our current page within this shard has more results
	 * 2.  Our current page within this shard is exhausted, but a future shard has results
	 * @return true if more results, false if not
	 */

	public boolean hasNext(){
		// If our current page within this shard has more results, we can just return true
		if(page != null && nextItem < pageSize && page.size() > nextItem) {
			return true;
		}
		// If not, we have to fetch more results and try again
		fetchIfNeeded();
		if(page != null && page.size() > nextItem) {
			return true;
		} else {
			return false;
		}
	}

	public Row next(){
		// If our current page within this shard has more results, just return one from this page
		if(page != null && nextItem < pageSize && page.size() > nextItem) {
			nextItem++;
			return page.get(nextItem - 1);
		}
		// If not, fetch more results and try again
		fetchIfNeeded();
		if(page != null && nextItem < pageSize && page.size() > nextItem) {
			nextItem++;
			return page.get(nextItem - 1);
		} else {
			return null;
		}
	}

	private void fetchIfNeeded() {
		// Just to be safe, make sure that we don't have anything remaining in the current page for this shard
		if(page != null && nextItem < pageSize && page.size() > nextItem) {
			return;
		}

		if (moreResultsInShard) {
			// If we are within a shard already, get more results from this shard
			Row row = page.get(nextItem);
			UUID uuid = row.getUUID("id");
			statementIterator.setNextUuid(uuid);
			CQLStatement cql = statementIterator.next();
			ResultSet resultSet = cqlExecutor.executeSync(cql);
			statementNumber++;
			populatePage(resultSet);
			if (page.size() > pageSize) {
				moreResultsInShard = true;
			} else {
				moreResultsInShard = false;
			}
			nextItem = 0;
		} else {
			// Reset our current shard state
			nextItem = 0;
			page = null;
			moreResultsInShard = false;

			// Start going through the remaining shards until we find one with results or hit the last one
			while (statementIterator.hasNext()) {
				// Move to the next shard in the iterator and reset the limits and stuff
				statementIterator.setLimit(pageSize + 1l);
				statementIterator.nextShard();
				statementIterator.setNextUuid(null);
				CQLStatement cql = statementIterator.next();
				ResultSet resultSet = cqlExecutor.executeSync(cql);
				statementNumber++;
				if (!resultSet.isExhausted()) {
					populatePage(resultSet);
					if (page.size() > pageSize) {
						moreResultsInShard = true;
					} else {
						moreResultsInShard = false;
					}
					return;
				}
			}
		}
	}

	private void populatePage(ResultSet resultSet){
		page = Lists.newArrayList();
		for(Row row : resultSet) {
			page.add(row);
		}
	}

	public void remove() {
		statementIterator.remove();
	}

	public Map<String,Object> getClientFilters() {
		return statementIterator.getClientFilters();
	}

	public void setClientFilters(Map<String, Object> clientFilters) {
		statementIterator.setClientFilters(clientFilters);
	}

	public void setPageSize(long pageSize){
		this.pageSize = pageSize;
	}

	public long getPageSize(){
		return pageSize;
	}
}
