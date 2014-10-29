package com.pardot.rhombus.functional;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.helpers.TestHelpers;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;

import static org.junit.Assert.*;

public class ObjectMapperITCaseWithTTL extends RhombusFunctionalTest {

	private static Logger logger = LoggerFactory.getLogger(ObjectMapperITCaseWithTTL.class);

	@Test
	public void testObjectMapperWithTTL() throws Exception {
		logger.debug("Starting testObjectMapperWithTTL");

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		assertNotNull(definition);

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper(definition.getName());

		//Get a test object to insert
		Map<String, Object> testObject = JsonUtil.rhombusMapFromJsonMap(TestHelpers.getTestObject(0), definition.getDefinitions().get("testtype"));
		UUID key = (UUID) om.insert("testtype", testObject);

		// Test that we can insert with TTL
		//Get a test object to insert
		Map<String, Object> testObject4 = JsonUtil.rhombusMapFromJsonMap(TestHelpers.getTestObject(0), definition.getDefinitions().get("testtype"));
		// This one should only persist for 4 seconds
		Integer ttl = 4;
		testObject4.put("_ttl", ttl);
		UUID key5 = (UUID) om.insert("testtype", testObject4);

		Map<String, Object> testObject5 = JsonUtil.rhombusMapFromJsonMap(TestHelpers.getTestObject(0), definition.getDefinitions().get("testtype"));
		// Whereas this one should persist for one day
		ttl = 86400;
		testObject5.put("_ttl", ttl);
		UUID key6 = (UUID) om.insert("testtype", testObject5);

		// Let's wait for five seconds
		Thread.sleep(5000);

		Map<String, Object> dbObject3 = om.getByKey("testtype", key5);
		// So the object created with key5 should be gone
		assertNull(dbObject3);
		Map<String, Object> dbObject4 = om.getByKey("testtype", key6);
		// Yet the object created with key6 should be extant
		assertNotNull(dbObject4);

		//Teardown connections
		cm.teardown();
	}

	@Test
	public void testLargeCountWithTTL() throws Exception {
		logger.debug("Starting testLargeCountWithTTL");

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "MultiInsertKeyspace.js");
		assertNotNull(definition);

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		logger.debug("Built keyspace: {}", definition.getName());
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setLogCql(true);

		//Set up test data
		int nDataItems = 200;

		List<Map<String, Object>> values2 = Lists.newArrayList();
		Integer ttl = 4;

		// insert additional data, we are testing for counts > 50
		for (int i = 0; i < nDataItems; i++) {
			Map<String, Object> value = Maps.newHashMap();
			value.put("account_id", "00000003-0000-0030-0040-000000030000");
			value.put("user_id", "00000003-0000-0030-0040-000000030000");
			value.put("field2", "Value" + (i + 8));
			values2.add(value);
		}

		List<Map<String, Object>> updatedValues2 = Lists.newArrayList();
		for (Map<String, Object> baseValue : values2) {
			Map<String, Object> value = JsonUtil.rhombusMapFromJsonMap(baseValue, definition.getDefinitions().get("object2"));
			value.put("_ttl", ttl);
			updatedValues2.add(value);
		}

		Map<String, List<Map<String, Object>>> multiInsertMap = Maps.newHashMap();
		multiInsertMap.put("object2", updatedValues2);


		//Insert data with 4-second TTL
		om.insertBatchMixed(multiInsertMap);

		// Sleep for five seconds
		Thread.sleep(5000);

		//Count the number of inserts we made
		SortedMap<String, Object> indexValues = Maps.newTreeMap();
		indexValues.put("account_id", UUID.fromString("00000003-0000-0030-0040-000000030000"));
		indexValues.put("user_id", UUID.fromString("00000003-0000-0030-0040-000000030000"));
		Criteria criteria = new Criteria();
		criteria.setIndexKeys(indexValues);

		// Should be zero.
		long count = om.count("object2", criteria);
		assertEquals(0, count);


		// Good.  Now let's make sure we aren't crazy.
		values2 = Lists.newArrayList();



		for (int i = 0; i < nDataItems; i++) {
			Map<String, Object> value = Maps.newHashMap();
			value.put("account_id", "00000003-0000-0030-0040-000000030000");
			value.put("user_id", "00000003-0000-0030-0040-000000030000");
			value.put("field2", "Value" + (i + 8));
			value.put("_ttl", ttl);
			values2.add(value);
		}

		Integer shortTimeToLive = 4;
		Integer longTimeToLive = 6;
		int numThatShouldBeExcluded = 15;
		int numThatShouldRemain = nDataItems - numThatShouldBeExcluded;
		int j = 0;

		updatedValues2 = Lists.newArrayList();
		for (Map<String, Object> baseValue : values2) {
			if (j < numThatShouldBeExcluded) {
				ttl = shortTimeToLive;
			}
			else {
				ttl = longTimeToLive;
			}
			j++;
			Map<String, Object> value = JsonUtil.rhombusMapFromJsonMap(baseValue, definition.getDefinitions().get("object2"));
			value.put("_ttl", ttl);
			updatedValues2.add(value);
		}

		multiInsertMap = Maps.newHashMap();
		multiInsertMap.put("object2", updatedValues2);


		//Insert data with 4-second TTL
		om.insertBatchMixed(multiInsertMap);

		// Sleep for five seconds
		Thread.sleep(5000);

		//Count the number of inserts we made
		indexValues = Maps.newTreeMap();
		indexValues.put("account_id", UUID.fromString("00000003-0000-0030-0040-000000030000"));
		indexValues.put("user_id", UUID.fromString("00000003-0000-0030-0040-000000030000"));
		criteria = new Criteria();
		criteria.setIndexKeys(indexValues);

		// Should be 185
		count = om.count("object2", criteria);
		assertEquals(numThatShouldRemain, count);

		cm.teardown();
	}

}
