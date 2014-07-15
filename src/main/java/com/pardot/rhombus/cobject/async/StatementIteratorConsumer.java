package com.pardot.rhombus.cobject.async;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.pardot.rhombus.RhombusException;
import com.pardot.rhombus.RhombusTimeoutException;
import com.pardot.rhombus.cobject.statement.BoundedCQLStatementIterator;
import com.pardot.rhombus.cobject.CQLExecutor;
import com.pardot.rhombus.cobject.statement.CQLStatement;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 6/21/13
 */
public class StatementIteratorConsumer {

	private static Logger logger = LoggerFactory.getLogger(StatementIteratorConsumer.class);
	private static ExecutorService executorService = Executors.newCachedThreadPool();

	private final BoundedCQLStatementIterator statementIterator;
	private CQLExecutor cqlExecutor;
	private final CountDownLatch shutdownLatch;
	private final long timeout;
	private final Set<Future> futures = Collections.synchronizedSet(new HashSet<Future>());
	private List<Throwable> executionExceptions = new Vector<Throwable>();

	public StatementIteratorConsumer(BoundedCQLStatementIterator statementIterator, CQLExecutor cqlExecutor, long timeout) {
		this.statementIterator = statementIterator;
		this.cqlExecutor = cqlExecutor;
		this.timeout = timeout;
		this.shutdownLatch = new CountDownLatch((new Long(statementIterator.size())).intValue());
		logger.trace("Created consumer with countdown {}", shutdownLatch.getCount());
	}

	public void start() {
		while(statementIterator.hasNext()) {
			final CQLStatement next = statementIterator.next();
			Runnable r = new Runnable() {
				@Override
				public void run() {
					handle(next);
				}
			};
			executorService.execute(r);
		}
	}

	public void join() throws RhombusException {
		logger.trace("Awaiting shutdownLatch with timeout {}ms", timeout);
		try {
			boolean complete = shutdownLatch.await(timeout, TimeUnit.MILLISECONDS);
			if(!complete) {
				Metrics.defaultRegistry().newMeter(StatementIteratorConsumer.class, "asyncTimeout", "asyncTimeout", TimeUnit.SECONDS).mark();
				cancelFutures();
				for(Throwable t : this.executionExceptions) {
					logger.warn("Timeout executing statements. Found future failure: ", t);
				}
				throw new RhombusTimeoutException("Timout executing statements asynch");
			}
			for(Throwable t : this.executionExceptions) {
				logger.warn("Completed executing statements, but found future failure: ", t);
			}
		} catch (InterruptedException e) {
			logger.warn("Interrupted while executing statements asynch", e);
			cancelFutures();
		}
	}

	private void cancelFutures() {
		for(Future future : futures) {
			try {
				future.cancel(true);
			} catch(Exception e) {
				logger.warn("Exception when cancelling future", e);
			}
		}
	}

	protected void handle(CQLStatement statement) {
		String methodName = "NULL";
		String cql = statement.getQuery();
		int firstSpace = cql.indexOf(" ");
		if(firstSpace > 0) {
			methodName = cql.substring(0, firstSpace);
		}
		String timerName = "asyncExec." + methodName + "." + statement.getObjectName();
		final Timer asyncExecTimer = Metrics.defaultRegistry().newTimer(StatementIteratorConsumer.class, timerName);
		final TimerContext asyncExecTimerContext = asyncExecTimer.time();
		final long startTime = System.nanoTime();
		ResultSetFuture future = null;
		try {
			future = this.cqlExecutor.executeAsync(statement);
		} catch (RuntimeException re) {
			logger.error("RuntimeException while executing statement {}\n {}", statement.getQuery(), re);
			shutdownLatch.countDown();
			return;
		}
		futures.add(future);
		Futures.addCallback(future, new FutureCallback<ResultSet>() {
			@Override
			public void onSuccess(final ResultSet result) {
				Host queriedHost = result.getExecutionInfo().getQueriedHost();
				Metrics.defaultRegistry().newMeter(StatementIteratorConsumer.class, "queriedhost." + queriedHost.getDatacenter(), queriedHost.getDatacenter(), TimeUnit.SECONDS).mark();
				asyncExecTimerContext.stop();
				logger.debug("Async exec time {}us", (System.nanoTime() - startTime) / 1000);
				shutdownLatch.countDown();
			}

			@Override
			public void onFailure(final Throwable t) {
				asyncExecTimerContext.stop();
				logger.debug("Async failure time {}us", (System.nanoTime() - startTime) / 1000);
				executionExceptions.add(t);
				shutdownLatch.countDown();
			}
		}
				, executorService
		);
	}
}
