package com.pardot.rhombus.functional;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.helpers.TestHelpers;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 7/25/13
 */
public class AsyncSpeedITCase extends RhombusFunctionalTest {

	private static Logger logger = LoggerFactory.getLogger(ObjectMapperITCase.class);

	@Test
	public void testAsyncSpeed() throws Exception {
		logger.debug("Starting testObjectMapper");

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();

		//Build our keyspace definition object
		String json = TestHelpers.readFileToString(this.getClass(), "CKeyspaceTestData.js");
		CKeyspaceDefinition definition = CKeyspaceDefinition.fromJsonString(json);
		assertNotNull(definition);

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		CDefinition def1 = om.getKeyspaceDefinition_ONLY_FOR_TESTING().getDefinitions().get("testtype");


		//do an insert on an object
		//make some objects to insert
		List<Map<String,Object>> toInserts = Lists.newArrayList();
		int numberOfObjects = 400;
		//int numberOfObjects = 100;
		for(int i = 0; i < numberOfObjects; i++){
			Map<String, Object> testObject = Maps.newTreeMap();
			testObject.put("foreignid", Long.valueOf(33339999));
			testObject.put("type", Integer.valueOf(i%5));
			testObject.put("instance", Long.valueOf(i%10));
			testObject.put("filtered", Integer.valueOf(1));
			testObject.put("data1", "This is data 1 "+i);
			testObject.put("data2", "This is data 2 "+i);
			testObject.put("data3", "This is data 3 "+i);
			toInserts.add(testObject);
		}

		Map<String,List<Map<String,Object>>> objects = Maps.newHashMap();
		objects.put("testtype", toInserts);

		long start = 0;
		long end = 0;
		long asyncTime = 0;
		long syncTime = 0;

		//insert sync
		om.setExecuteAsync(false);
		start = System.currentTimeMillis();
		om.insertBatchMixed(objects);
		end = System.currentTimeMillis();
		syncTime = end - start;

		//Rebuild the keyspace and get the object mapper
		cm = getConnectionManager();
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		om = cm.getObjectMapper();
		def1 = om.getKeyspaceDefinition_ONLY_FOR_TESTING().getDefinitions().get("testtype");


		//insert async
		om.setExecuteAsync(true);
		start = System.currentTimeMillis();
		om.insertBatchMixed(objects);
		end = System.currentTimeMillis();
		asyncTime = end - start;




		logger.info("Insert Speed Results");
		logger.info("======================");
		logger.info("Sync Time ms: " + syncTime);
		logger.info("Async Time ms: " + asyncTime);

		//testObject.put("foreignid", Long.valueOf(33339999));
		//Query it back out
		//Make sure that we have the proper number of results
		SortedMap<String, Object> indexValues = Maps.newTreeMap();
		indexValues.put("foreignid", Long.valueOf(33339999));
		Criteria criteria = new Criteria();
		criteria.setIndexKeys(indexValues);
		Thread.sleep(4000);
		int count = om.list("testtype",criteria).size();
		logger.info("======================");
		logger.info("Retrieved count of " + count);
		logger.info("======================");
		assertEquals(numberOfObjects,count);

		cm.teardown();
	}

}
