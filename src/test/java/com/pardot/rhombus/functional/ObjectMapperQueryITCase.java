package com.pardot.rhombus.functional;


import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Maps;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.RhombusException;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.cobject.CQLGenerationException;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class ObjectMapperQueryITCase extends RhombusFunctionalTest {

	private static Logger logger = LoggerFactory.getLogger(ObjectMapperQueryITCase.class);

	long batchSize = 200;
	String objectType = "object1";
	UUID accountId = UUIDs.random();
	UUID userId = UUIDs.random();

	@Test
	public void testGetLessThanLimitInSingleShard() throws Exception {
		//Build the connection manager
		ConnectionManager cm = getConnectionManager();
		cm.setLogCql(true);

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "ShardedKeyspace.js");

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setLogCql(true);

		long insertNum = 10l;

		insertNObjects(om, insertNum, batchSize);

		// Get 50 back
		long limit = 50l;
		Criteria criteria = new Criteria();
		SortedMap<String, Object> indexKeys = Maps.newTreeMap();
		indexKeys.put("account_id", accountId);
		indexKeys.put("user_id", userId);
		criteria.setIndexKeys(indexKeys);
		criteria.setLimit(limit);
		List<Map<String, Object>> results = om.list(objectType, criteria);

		assertEquals(insertNum, results.size());

		cm.teardown();
	}

	@Test
	public void testGetInEmptyShardList() throws Exception {
		//Build the connection manager
		ConnectionManager cm = getConnectionManager();
		cm.setLogCql(true);

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "ShardedKeyspace.js");

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setLogCql(true);

		// Get 50 back
		long limit = 50l;
		Criteria criteria = new Criteria();
		SortedMap<String, Object> indexKeys = Maps.newTreeMap();
		indexKeys.put("account_id", accountId);
		indexKeys.put("user_id", userId);
		criteria.setIndexKeys(indexKeys);
		criteria.setLimit(limit);
		List<Map<String, Object>> results = om.list(objectType, criteria);

		assertEquals(0, results.size());

		cm.teardown();
	}

	public void insertNObjects(ObjectMapper om, long number, long batchSize) throws RhombusException, CQLGenerationException {
		if(number < batchSize) {
			batchSize = number;
		}
		long numInserted = 0;
		while(numInserted < number) {
			long numLeft = number - numInserted;
			long numToInsert = batchSize;
			if(numLeft < batchSize) {
				numToInsert = numLeft;
			}
			List<Map<String, Object>> objects = getNValues(numToInsert, accountId, userId);
			Map<String, List<Map<String, Object>>> batch = Maps.newHashMap();
			batch.put(objectType, objects);
			om.insertBatchMixed(batch);
			numInserted += numToInsert;
		}
	}

	private List<Map<String, Object>> getNValues(long number, UUID index1Value, UUID index2Value) {
		List<Map<String, Object>> values = new ArrayList<Map<String, Object>>();
		for(int i = 0 ; i < number ; i++) {
			Map<String, Object> value = Maps.newHashMap();
			value.put("account_id", index1Value);
			value.put("user_id", index2Value);
			values.add(value);
		}
		return values;
	}
}
