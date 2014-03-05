package com.pardot.rhombus.cobject;

import com.datastax.driver.core.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.cobject.statement.CQLStatement;
import com.pardot.rhombus.cobject.statement.CQLStatementIterator;
import com.yammer.metrics.core.TimerContext;
import com.yammer.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.pardot.rhombus.util.StringUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 6/28/13
 */
public class CQLExecutor {

	private Map<String, PreparedStatement> preparedStatementCache;
	private static Logger logger = LoggerFactory.getLogger(CQLExecutor.class);
	private boolean logCql = false;
	private boolean enableTrace = false;
	private Session session;
	private ConsistencyLevel consistencyLevel;

	public CQLExecutor(Session session, boolean logCql, ConsistencyLevel consistencyLevel){
		this.preparedStatementCache = Maps.newConcurrentMap();
		this.session = session;
		this.logCql = logCql;
		this.consistencyLevel = consistencyLevel;
	}

	public void clearStatementCache(){
		preparedStatementCache.clear();
	}

	public Map<String, PreparedStatement> getPreparedStatementCache() {
		return preparedStatementCache;
	}

	public BoundStatement getBoundStatement(Session session, CQLStatement cql){
		PreparedStatement ps = preparedStatementCache.get(cql.getQuery());
		if(ps == null){
			ps = prepareStatement(session, cql);
		}
		BoundStatement ret = new BoundStatement(ps);
		ret.bind(cql.getValues());
		if(enableTrace) {
			ret.enableTracing();
		}
		return ret;
	}

    public PreparedStatement prepareStatement(Session session, CQLStatement cql){
		if(preparedStatementCache.containsKey(cql.getQuery())) {
			// When pre-preparing statements, we can send the same one multiple times
			// in this case, we should just return the one from the cache and not prepare again
			return preparedStatementCache.get(cql.getQuery());
		} else {
			Long currentTime = System.currentTimeMillis();
			logger.warn("Preparing statement {}", cql.getQuery());
			TimerContext prepareTimer = Metrics.defaultRegistry().newTimer(CQLExecutor.class, "statement.prepared").time();
			PreparedStatement ret = session.prepare(cql.getQuery());
			prepareTimer.stop();
			logger.warn("Completed Preparing statement {} in {}ms", cql.getQuery(), System.currentTimeMillis()-currentTime);
			ret.setConsistencyLevel(consistencyLevel);
			preparedStatementCache.put(cql.getQuery(), ret);
			return ret;
		}
    }

	public ResultSet executeSync(CQLStatement cql){
		if(logCql) {
			logger.debug("Executing CQL: {}", cql.getQuery());
			if(cql.getValues() != null) {
				logger.debug("With values: {}", StringUtil.detailedListToString(Arrays.asList(cql.getValues())));
			}
		}
		if(cql.isPreparable()){
			BoundStatement bs = getBoundStatement(session, cql);
			return session.execute(bs);
		}
		else{
			//just run a normal execute without a prepared statement
			return session.execute(cql.getQuery());
		}
	}

	public ResultSet executeSync(Statement cql){
		if(logCql) {
			logger.debug("Executing QueryBuilder Query: {}", cql.toString());
		}
		//just run a normal execute without a prepared statement
		return session.execute(cql);
	}

	public ResultSetFuture executeAsync(CQLStatement cql){
		if(logCql) {
			logger.debug("Executing CQL: {}", cql.getQuery());
			if(cql.getValues() != null) {
				logger.debug("With values: {}", Arrays.asList(cql.getValues()));
			}
		}
		if(cql.isPreparable()){
			BoundStatement bs = getBoundStatement(session, cql);
			ResultSetFuture result = session.executeAsync(bs);
			com.yammer.metrics.Metrics.defaultRegistry().newMeter(CQLExecutor.class, "statement.executed", "executed", TimeUnit.SECONDS).mark();
			return result;
		}
		else{
			//just run a normal execute without a prepared statement
			com.yammer.metrics.Metrics.defaultRegistry().newMeter(CQLExecutor.class, "statement.executed", "executed", TimeUnit.SECONDS).mark();
			return session.executeAsync(cql.getQuery());
		}
	}

	public void executeBatch(List<CQLStatementIterator> statementIterators) {
		BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
		for(CQLStatementIterator statementIterator : statementIterators) {
			while(statementIterator.hasNext()) {
				CQLStatement statement = statementIterator.next();
				batchStatement.add(getBoundStatement(session, statement));
			}
		}
		session.execute(batchStatement);
	}

	public void executeBatch(CQLStatementIterator statementIterator) {
		List<CQLStatementIterator> statementIterators = Lists.newArrayList();
		statementIterators.add(statementIterator);
		executeBatch(statementIterators);
	}

	public boolean isLogCql() {
		return logCql;
	}

	public void setLogCql(boolean logCql) {
		this.logCql = logCql;
	}

	public boolean isEnableTrace() {
		return enableTrace;
	}

	public void setEnableTrace(boolean enableTrace) {
		this.enableTrace = enableTrace;
	}
}
