package com.pardot.rhombus;

import com.datastax.driver.core.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CObjectTokenVisitor;
import com.pardot.rhombus.cobject.CObjectTokenVisitorFactory;
import com.pardot.rhombus.cobject.statement.CQLStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: Michael Frank
 * Date: 6/30/14
 */
public class TableScanner {

	private static Logger logger = LoggerFactory.getLogger(TableScanner.class);

	private long minToken = Long.MIN_VALUE;
	private long maxToken = Long.MAX_VALUE;
	private long batchSize = 200;
	private int statementRetries = 5;

	private ObjectMapper objectMapper;
	private String objectType;
	private int numPartitions;
	CObjectTokenVisitorFactory visitorFactory;
	private ExecutorService executorService;
	private final CountDownLatch shutdownLatch;

	public TableScanner(ObjectMapper objectMapper, String objectType, int numPartitions, CObjectTokenVisitorFactory visitorFactory) {
		Preconditions.checkNotNull(objectMapper);
		Preconditions.checkNotNull(objectType);
		Preconditions.checkNotNull(numPartitions);
		Preconditions.checkNotNull(visitorFactory);
		this.objectMapper = objectMapper;
		this.objectType = objectType;
		this.numPartitions = numPartitions;
		this.visitorFactory = visitorFactory;
		executorService = Executors.newFixedThreadPool(numPartitions);
		shutdownLatch = new CountDownLatch(numPartitions);
	}

	public void scan() {
		List<Map.Entry<Long, Long>> ranges = makeRanges();
		int rangeNum = 0;
		for(final Map.Entry<Long, Long> range : ranges) {
			rangeNum++;
			final CObjectTokenVisitor visitor = visitorFactory.getInstance("" + rangeNum);
			Runnable r = new Runnable() {
				@Override
				public void run() {
					handle(range.getKey(), range.getValue(), visitor);
				}
			};
			executorService.execute(r);
		}
		try {
			shutdownLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public List<Map.Entry<Long, Long>> makeRanges() {
		List<Map.Entry<Long, Long>> ranges = Lists.newArrayList();
		BigInteger fullRange = BigInteger.valueOf(maxToken).subtract(BigInteger.valueOf(minToken));
		BigInteger rangeLength = fullRange.divide(BigInteger.valueOf(numPartitions));
		BigInteger rangeStart = BigInteger.valueOf(minToken);
		for(int i = 0 ; i < numPartitions - 1 ; i++) {
			BigInteger rangeEnd = rangeStart.add(rangeLength).subtract(BigInteger.ONE);
			AbstractMap.SimpleEntry<Long, Long> range = new AbstractMap.SimpleEntry<Long, Long>(rangeStart.longValue(), rangeEnd.longValue());
			ranges.add(range);
			rangeStart = rangeEnd.add(BigInteger.ONE);
		}
		BigInteger rangeEnd = BigInteger.valueOf(maxToken);
		AbstractMap.SimpleEntry<Long, Long> range = new AbstractMap.SimpleEntry<Long, Long>(rangeStart.longValue(), rangeEnd.longValue());
		ranges.add(range);
		return ranges;
	}

	public void handle(Long minToken, Long maxToken, CObjectTokenVisitor visitor) {
		List<Map<String, Object>> results = executeStatement(objectType, minToken, maxToken, batchSize);
		while(results != null && results.size() > 0) {
			visitResults(results, visitor);
			String minUuid = String.valueOf(results.get(results.size() - 1).get("id"));
			results = executeStatement(objectType, minUuid, maxToken, batchSize);
		}
		shutdownLatch.countDown();
	}

	private void visitResults(List<Map<String, Object>> results, CObjectTokenVisitor visitor) {
		for(Map<String, Object> result : results) {
			visitor.visit(result);
		}
	}

	private List<Map<String, Object>> executeStatement(String objectType, Object minToken, Long maxToken, long batchSize) {
		int retries = 0;
		while(retries < statementRetries) {
			try {
				List<Map<String, Object>> results;
				if(Long.class.isAssignableFrom(minToken.getClass())) {
					results = objectMapper.scanTableWithStartToken(objectType, (Long)minToken, maxToken, batchSize);
				} else {
					results = objectMapper.scanTableWithStartId(objectType, (String)minToken, maxToken, batchSize);
				}
				return results;
			} catch(Exception e) {
				logger.error("Exception number " + retries + " executing statement", e);
				retries++;
				if(retries >= statementRetries) {
					throw new RuntimeException(e);
				}
			}
		}
		throw new RuntimeException("Bad logic in statement retry");
	}

	public long getMinToken() {
		return minToken;
	}

	public void setMinToken(long minToken) {
		this.minToken = minToken;
	}

	public long getMaxToken() {
		return maxToken;
	}

	public void setMaxToken(long maxToken) {
		this.maxToken = maxToken;
	}

	public long getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public int getStatementRetries() {
		return statementRetries;
	}

	public void setStatementRetries(int statementRetries) {
		this.statementRetries = statementRetries;
	}
}
