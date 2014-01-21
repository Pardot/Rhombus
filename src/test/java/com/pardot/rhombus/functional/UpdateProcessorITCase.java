package com.pardot.rhombus.functional;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.UpdateProcessor;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CIndex;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.cobject.CQLStatement;
import com.pardot.rhombus.helpers.TestHelpers;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 7/19/13
 */
public class UpdateProcessorITCase extends RhombusFunctionalTest {

	private static Logger logger = LoggerFactory.getLogger(ObjectMapperITCase.class);

	@Test
	public void testUpdateProcessor() throws Exception {
		logger.debug("Starting testObjectMapper");

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();

		//Build our keyspace definition object
		String json = TestHelpers.readFileToString(this.getClass(), "CKeyspaceTestData.js");
		CKeyspaceDefinition definition = CKeyspaceDefinition.fromJsonString(json);
		String keyspace = definition.getName();
		assertNotNull(definition);

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.truncateTables();

		CDefinition def1 = om.getKeyspaceDefinition_ONLY_FOR_TESTING().getDefinitions().get("testtype");
		//do an insert on an object
		Map<String, Object> testObject = Maps.newTreeMap();
		testObject.put("foreignid", Long.valueOf(100));
		testObject.put("type", Integer.valueOf(101));
		testObject.put("instance", Long.valueOf(102));
		testObject.put("filtered", Integer.valueOf(103));
		testObject.put("data1", "This is data 1");
		testObject.put("data2", "This is data 2");
		testObject.put("data3", "This is data 3");

		UUID key = (UUID)om.insert("testtype", testObject);

		testObject.put("foreignid", Long.valueOf(200));
		testObject.put("type", Integer.valueOf(201));
		testObject.put("instance", Long.valueOf(202));
		testObject.put("filtered", Integer.valueOf(203));

		//manually insert that object incorrectly into other indexes
		List<CQLStatement> insertStatements = Lists.newArrayList();
		for(CIndex i : def1.getIndexes().values()){
			om.getCqlGenerator_ONLY_FOR_TESTING().addCQLStatmentsForIndexInsert(
					keyspace,
					true,
					insertStatements,
					def1,
					testObject,
					i,
					key,
					om.getCqlGenerator_ONLY_FOR_TESTING().makeFieldAndValueList(def1, testObject), null, null);
		}
		for(CQLStatement s: insertStatements){
			om.getCqlExecutor().executeSync(s);
		}

		//manually record those incorrect values in the update table
		CQLStatement cql = om.getCqlGenerator_ONLY_FOR_TESTING().makeInsertUpdateIndexStatement(
				keyspace,
				def1,
				key, def1.makeIndexValues(testObject));
		om.getCqlExecutor().executeSync(cql);

		//now manually record an update back to the original in the update table to simulate an eventual consistency issue
		Map<String, Object> testObjectOriginal = Maps.newTreeMap();
		testObjectOriginal.put("foreignid", Long.valueOf(100));
		testObjectOriginal.put("type", Integer.valueOf(101));
		testObjectOriginal.put("instance", Long.valueOf(102));
		testObjectOriginal.put("filtered", Integer.valueOf(103));
		testObjectOriginal.put("data1", "This is data 1");
		testObjectOriginal.put("data2", "This is data 2");
		testObjectOriginal.put("data3", "This is data 3");
		cql = om.getCqlGenerator_ONLY_FOR_TESTING().makeInsertUpdateIndexStatement(
				definition.getName(),
				def1,
				key, def1.makeIndexValues(testObjectOriginal));
		om.getCqlExecutor().executeSync(cql);

		//verify that the object returns different values in the static table and on those (or some of those) indexes
		Map<String, Object> staticTableObject = om.getByKey("testtype", key);
		assertEquals(100L,staticTableObject.get("foreignid"));
		assertEquals(101,staticTableObject.get("type"));
		assertEquals(103,staticTableObject.get("filtered"));
		assertEquals("This is data 1",staticTableObject.get("data1"));

		Criteria criteria = new Criteria();
		SortedMap<String,Object> values = Maps.newTreeMap();
		values.put("foreignid", Long.valueOf(200L));
		criteria.setIndexKeys(values);
		criteria.setLimit(0L);
		List<Map<String, Object>> indexObjects = om.list("testtype", criteria);
		assertEquals(1, indexObjects.size());
		assertEquals(staticTableObject.get("data1"),indexObjects.get(0).get("data1"));
		assertEquals(200L,indexObjects.get(0).get("foreignid"));
		assertEquals(201,indexObjects.get(0).get("type"));
		assertEquals(203,indexObjects.get(0).get("filtered"));


		//wait for consistency
		Thread.sleep(3000);

		//now run the processor
		UpdateProcessor up = new UpdateProcessor(om);
		up.process();

		//verify that the object is no longer present in the invalid indexes

		//Should be missing from the bad index
		criteria = new Criteria();
		values = Maps.newTreeMap();
		values.put("foreignid", Long.valueOf(200));
		criteria.setIndexKeys(values);
		criteria.setLimit(0L);
		indexObjects = om.list("testtype", criteria);
		assertEquals(0, indexObjects.size());

		//But is should be present in the correct index
		criteria = new Criteria();
		values = Maps.newTreeMap();
		values.put("foreignid", Long.valueOf(100));
		criteria.setIndexKeys(values);
		indexObjects = om.list("testtype", criteria);
		assertEquals(1, indexObjects.size());
		assertEquals(staticTableObject.get("data1"),indexObjects.get(0).get("data1"));
		assertEquals(100L,indexObjects.get(0).get("foreignid"));
		assertEquals(101,indexObjects.get(0).get("type"));
		assertEquals(103,indexObjects.get(0).get("filtered"));
	}




	@Test
	public void testGetUpdatesThatHappenedWithinTimeframe() throws Exception {
		logger.debug("Starting testObjectMapper");

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();

		//Build our keyspace definition object
		String json = TestHelpers.readFileToString(this.getClass(), "CKeyspaceTestData.js");
		CKeyspaceDefinition definition = CKeyspaceDefinition.fromJsonString(json);
		String keyspace = definition.getName();
		assertNotNull(definition);

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.truncateTables();

		//do an insert on an object
		Map<String, Object> testObject = Maps.newTreeMap();
		testObject.put("foreignid", Long.valueOf(100));
		testObject.put("type", Integer.valueOf(101));
		testObject.put("instance", Long.valueOf(102));
		testObject.put("filtered", Integer.valueOf(103));
		testObject.put("data1", "This is data 1");
		testObject.put("data2", "This is data 2");
		testObject.put("data3", "This is data 3");

		UUID key = (UUID)om.insert("testtype", testObject);

		Map<String, Object> updateObj = Maps.newTreeMap();
		updateObj.put("foreignid", Long.valueOf(1));
		om.update("testtype",key, updateObj);

		updateObj = Maps.newTreeMap();
		updateObj.put("foreignid", Long.valueOf(2));
		om.update("testtype",key, updateObj);

		updateObj = Maps.newTreeMap();
		updateObj.put("foreignid", Long.valueOf(3));
		om.update("testtype",key, updateObj);

		updateObj = Maps.newTreeMap();
		updateObj.put("foreignid", Long.valueOf(4));
		om.update("testtype",key, updateObj);

		updateObj = Maps.newTreeMap();
		updateObj.put("foreignid", Long.valueOf(5));
		om.update("testtype",key, updateObj);

		Thread.sleep(2000);

		updateObj = Maps.newTreeMap();
		updateObj.put("foreignid", Long.valueOf(6));
		om.update("testtype",key, updateObj);

		Thread.sleep(2000);



		UpdateProcessor up = new UpdateProcessor(om);
		//Test that we only see 4 happening within 90 milliseconds of each other
		assertEquals(4, up.getUpdatesThatHappenedWithinTimeframe(900000L).size()); //90 milliseconds

		//Test that we can see all 5 diffs when we search for those happening within 9 seconds of each other
		assertEquals(5, up.getUpdatesThatHappenedWithinTimeframe(900000L * 1000).size()); //9 seconds
	}


	@Test
	public void testUpdateRowLimit() throws Exception {
		logger.debug("Starting testObjectMapper");

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();

		//Build our keyspace definition object
		String json = TestHelpers.readFileToString(this.getClass(), "CKeyspaceTestData.js");
		CKeyspaceDefinition definition = CKeyspaceDefinition.fromJsonString(json);
		String keyspace = definition.getName();
		assertNotNull(definition);

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, false);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.truncateTables();

		//do an insert on an object
		Map<String, Object> testObject = Maps.newTreeMap();
		testObject.put("foreignid", Long.valueOf(100));
		testObject.put("type", Integer.valueOf(101));
		testObject.put("instance", Long.valueOf(102));
		testObject.put("filtered", Integer.valueOf(103));
		testObject.put("data1", "This is data 1");
		testObject.put("data2", "This is data 2");
		testObject.put("data3", "This is data 3");

		Map<String, Object> testObject2 = Maps.newTreeMap();
		testObject2.put("foreignid", Long.valueOf(200));
		testObject2.put("type", Integer.valueOf(201));
		testObject2.put("instance", Long.valueOf(202));
		testObject2.put("filtered", Integer.valueOf(203));
		testObject2.put("data1", "This is data 2-1");
		testObject2.put("data2", "This is data 2-2");
		testObject2.put("data3", "This is data 2-3");

		UUID key = (UUID)om.insert("testtype", testObject);
		Map<String, Object> updateObj = Maps.newTreeMap();
		updateObj.put("foreignid", 1l);
		om.update("testtype", key, updateObj);

		updateObj = Maps.newTreeMap();
		updateObj.put("foreignid", 2l);
		om.update("testtype", key, updateObj);

		UUID key2 = (UUID)om.insert("testtype", testObject2);
		updateObj = Maps.newTreeMap();
		updateObj.put("foreignid", 4l);
		om.update("testtype", key2, updateObj);

		updateObj = Maps.newTreeMap();
		updateObj.put("foreignid", 5l);
		om.update("testtype", key2, updateObj);

		Thread.sleep(3000);

		// Test row examine limit
		UpdateProcessor up = new UpdateProcessor(om);
		assertEquals(1, up.getUpdatesThatHappenedWithinTimeframe(900000L, 1).size());
		assertEquals(2, up.getUpdatesThatHappenedWithinTimeframe(900000L, 2).size());
	}

}
