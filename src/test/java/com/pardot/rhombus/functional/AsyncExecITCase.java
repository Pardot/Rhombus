package com.pardot.rhombus.functional;


import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.helpers.TestHelpers;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;

import static org.junit.Assert.*;

public class AsyncExecITCase extends RhombusFunctionalTest{

	private static Logger logger = LoggerFactory.getLogger(AsyncExecITCase.class);

	@Test
	public void testInsertAsyncMulti() throws Exception {
		logger.debug("Starting testInsertAsync");

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "AuditKeyspace.js");
		assertNotNull(definition);

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		logger.debug("Built keyspace: {}", definition.getName());
		cm.setDefaultKeyspace(definition);
		final ObjectMapper om = cm.getObjectMapper();

		final int numThreads = 10;
		final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
		List<Map<String, Object>> values = JsonUtil.rhombusMapFromResource(this.getClass().getClassLoader(), "DateRangeQueryTestData.js");
		final CDefinition objectAuditDef = definition.getDefinitions().get("object_audit");

		//Run through the inserts a few times and get a total time to execute
		long startTime = System.currentTimeMillis();
		for(int i = 0 ; i < 100 ; i++) {
			insertObjectSetAsync(numThreads, executorService, om, values, objectAuditDef);
		}
		logger.warn("Total time: {}ms", System.currentTimeMillis() - startTime);
	}

	private void insertObjectSetAsync(int numThreads, ExecutorService executorService, final ObjectMapper om, final List<Map<String, Object>> values, final CDefinition objectAuditDef) {
		final CountDownLatch latch = new CountDownLatch(numThreads);
		for(int i = 0 ; i < numThreads ; i++) {
			Runnable r = new Runnable() {
				@Override
				public void run() {
					try {
						for(Map<String, Object> object : values) {
							Long createdAt = (Long)(object.get("created_at"));
							SortedMap<String, Object> rhombusMap = JsonUtil.rhombusMapFromJsonMap(object, objectAuditDef);
							om.insert("object_audit", rhombusMap, createdAt);
						}
					} catch (Exception e) {
						logger.error("Error inserting", e);
					} finally {
						latch.countDown();
					}
				}
			};
			executorService.execute(r);
		}
		awaitUninterruptibly(latch, 25, TimeUnit.SECONDS);
	}

}
