package com.pardot.rhombus.functional;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.collect.Maps;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.*;
import com.pardot.rhombus.cobject.shardingstrategy.ShardingStrategyNone;
import com.pardot.rhombus.cobject.statement.CQLStatement;
import com.pardot.rhombus.helpers.ConnectionManagerTester;
import com.pardot.rhombus.helpers.TestHelpers;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class ConnectionManagerITCase extends RhombusFunctionalTest {

	private static Logger logger = LoggerFactory.getLogger(ConnectionManagerITCase.class);

	@Test
	public void testBuildCluster() throws Exception {
		logger.debug("testBuildCluster");
		//Get a connection manager based on the test properties
		ConnectionManager connectionManager = TestHelpers.getTestConnectionManager();
		connectionManager.setLogCql(true);
		Cluster cassCluster = connectionManager.buildCluster(true);
		assertNotNull(connectionManager);

		assertEquals(1,cassCluster.getMetadata().getAllHosts().size());
		assertEquals(cassCluster.getMetadata().getClusterName(), "Test Cluster");
	}

	@Test
	public void testBuildKeyspace() throws Exception {
		logger.debug("testBuildKeyspace");
		// Set up a connection manager and build the cluster
		ConnectionManager cm = getConnectionManager();

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class
				, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		assertNotNull(definition);

		//Build the same keyspace but do not force a rebuild
		cm.buildKeyspace(definition, false);

		//verify that we saved the keyspace definition in cassandra correctly
		CKeyspaceDefinition queriedDef = cm.hydrateLatestKeyspaceDefinitionFromCassandra(definition.getName());
		assertEquals(definition.getDefinitions().size(), queriedDef.getDefinitions().size());
		assertEquals(queriedDef.getDefinitions().get("testtype").getField("foreignid").getType(), CField.CDataType.BIGINT);
	}

	@Test
	public void testForceRebuild() throws Exception {
		logger.debug("testForceRebuild");
		// Set up a connection manager and build the cluster
		ConnectionManager cm = getConnectionManager();

		//Build the keyspace forcing a rebuild in case anything has been left behind
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class
				, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		assertNotNull(definition);
		cm.buildKeyspace(definition, true);

		//Build the keyspace without forcing a rebuild, but adding another index
		CKeyspaceDefinition definition2 = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class
				, this.getClass().getClassLoader(), "CKeyspaceTestData2.js");
		assertNotNull(definition2);
		cm.buildKeyspace(definition2, false);

		//Select from the newly created table
		ObjectMapper om = cm.getObjectMapper(definition2);
		ResultSet rs = om.getCqlExecutor().executeSync(CQLStatement.make("SELECT * FROM testtype3cb23c7ffc4256283064bd5eae1886b4", "testtype"));
		assertEquals(0, rs.all().size());
	}

	@Test
	public void testDropKeyspace() throws Exception {
		// Set up a connection manager and build the cluster and keyspace
		ConnectionManager cm = getConnectionManager();
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class
				, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		assertNotNull(definition);
		cm.buildKeyspace(definition, false);

		// Drop the keyspace
		cm.dropKeyspace(definition.getName());

		// Make sure it is really dropped
		Session session = cm.getEmptySession();
		boolean caught = false;
		try {
			session.execute("USE " + definition.getName() + ";");
		} catch(InvalidQueryException e) {
			caught = true;
		}
		session.close();
		assertTrue(caught);
	}

	@Test
	public void testRhombusKeyspaceCreatedIfNotExists() throws Exception {
		// Set up a connection manager
		ConnectionManagerTester cm = getConnectionManager();

		// Manually drop our Rhombus keyspace
		cm.dropKeyspace(cm.getRhombusKeyspaceName());

		// Get the Rhombus object mapper
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class
				, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		ObjectMapper om = cm.getRhombusObjectMapper(definition);
		assertNotNull(om);
	}

	@Test
	public void testKeyspaceCreatedFromDefinitionIfNotExists() throws Exception {
		// Set up a connection manager
		ConnectionManagerTester cm = getConnectionManager();

		// Manually drop our Rhombus keyspace
		cm.dropKeyspace(cm.getRhombusKeyspaceName());

		// Manually drop the functional keyspace if it exists
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class
				, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		cm.dropKeyspace(definition.getName());

		// Try to get an object mapper for the functional keyspace
		cm.getObjectMapper(definition);
		assertNotNull(definition);

		// Make sure we have stored the keyspace definition in our Rhombus keyspace
		ObjectMapper rhombusObjectMapper = cm.getRhombusObjectMapper(definition);
		CKeyspaceDefinition createdDefinition = rhombusObjectMapper.hydrateRhombusKeyspaceDefinition(definition.getName());
		assertEquals(definition, createdDefinition);

		// Close down our ConnectionManager, make a new one, and verify that we get the proper keyspace
		cm.teardown();
		cm = getConnectionManager();
		ObjectMapper defObjectMapper = cm.getObjectMapper(definition);
		assertEquals(definition, defObjectMapper.getKeyspaceDefinition_ONLY_FOR_TESTING());
	}

	@Test
	public void testPreferRhombusStorageDefinition() throws Exception {
		// Set up a connection manager
		ConnectionManagerTester cm = getConnectionManager();

		// Manually drop our Rhombus keyspace
		cm.dropKeyspace(cm.getRhombusKeyspaceName());

		// Manually drop the functional keyspace if it exists
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class
				, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		cm.dropKeyspace(definition.getName());

		// Build the keyspace from the base definition
		cm.buildKeyspace(definition, false);

		// Simulate a new process interacting with this Rhombus data by tearing down the cluster
		// and getting a new connection manager
		cm.teardown();
		cm = getConnectionManager();

		// Make a slightly altered new definition
		CKeyspaceDefinition definition2 = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class
				, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		CDefinition keyspaceDef = definition2.getDefinitions().values().iterator().next();
		Map<String, CField> fields = keyspaceDef.getFields();
		fields.put("new_field", new CField("new_field", CField.CDataType.ASCII));
		assertNotEquals(definition, definition2);

		ObjectMapper om = cm.getObjectMapper(definition2);
		CKeyspaceDefinition omDef = om.getKeyspaceDefinition_ONLY_FOR_TESTING();
		assertEquals(definition, omDef);
	}

	@Test
	public void testGetKeyspaceFromRhombusStorage() throws Exception {
		// Set up a connection manager
		ConnectionManagerTester cm = getConnectionManager();

		// Manually drop our Rhombus keyspace
		cm.dropKeyspace(cm.getRhombusKeyspaceName());

		// Manually drop the functional keyspace if it exists
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class
				, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		cm.dropKeyspace(definition.getName());

		// Build the keyspace from the base definition
		cm.buildKeyspace(definition, false);

		// Simulate a new process interacting with this Rhombus data by tearing down the cluster
		// and getting a new connection manager
		cm.teardown();
		cm = getConnectionManager();

		ObjectMapper om = cm.getObjectMapper(definition.getName());
		CKeyspaceDefinition rhombusStorageKeyspaceDefinition = om.getKeyspaceDefinition_ONLY_FOR_TESTING();
		assertEquals(definition, rhombusStorageKeyspaceDefinition);
	}

	@Test
	public void testKeyspaceDefinitionMigration() throws Exception {
		CKeyspaceDefinition OldKeyspaceDefinition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		CKeyspaceDefinition NewKeyspaceDefinition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		//add a new index to existing object
		CIndex newIndex1 = new CIndex();
		newIndex1.setKey("data1:data2");
		newIndex1.setShardingStrategy(new ShardingStrategyNone());
		NewKeyspaceDefinition.getDefinitions().get("testtype").getIndexes().put(newIndex1.getName(), newIndex1);
		//add new object
		CDefinition NewObjectDefinition = JsonUtil.objectFromJsonResource(CDefinition.class, this.getClass().getClassLoader(), "MigrationTestCDefinition.js");
		NewKeyspaceDefinition.getDefinitions().put(NewObjectDefinition.getName(),NewObjectDefinition);

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();

		//This test requires that the keyspace be dropped before running
		cm.dropKeyspace(OldKeyspaceDefinition.getName());
		cm.dropKeyspace(cm.getRhombusKeyspaceName());

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(OldKeyspaceDefinition, true);
		ObjectMapper om = cm.getObjectMapper(OldKeyspaceDefinition.getName());

		//insert some data
		//Get a test object to insert
		Map<String, Object> testObject = JsonUtil.rhombusMapFromJsonMap(TestHelpers.getTestObject(0), OldKeyspaceDefinition.getDefinitions().get("testtype"));
		UUID key = (UUID)om.insert("testtype", testObject);

		//Query to get back the object from the database
		Map<String, Object> dbObject = om.getByKey("testtype", key);
		for(String dbKey : dbObject.keySet()) {
			//Verify that everything but the key is the same
			if(!dbKey.equals("id")) {
				assertEquals(testObject.get(dbKey), dbObject.get(dbKey));
			}
		}

		//run the migration grabbing a brand new object mapper
		cm = getConnectionManager();
		om = cm.getObjectMapper(NewKeyspaceDefinition);

		//make sure that our keyspace definitions do not match
		assertNotEquals(NewKeyspaceDefinition, cm.hydrateLatestKeyspaceDefinitionFromCassandra(NewKeyspaceDefinition.getName()));
		assertNotEquals(NewKeyspaceDefinition, om.getKeyspaceDefinition_ONLY_FOR_TESTING());

		//run the migration
		cm.runMigration(NewKeyspaceDefinition, true);

		//make sure that the object mapper has the new keyspace definition
		CKeyspaceDefinition updatedKeyspaceDefinition = om.getKeyspaceDefinition_ONLY_FOR_TESTING();
		assertEquals(NewKeyspaceDefinition, updatedKeyspaceDefinition);

		//make sure that the new keyspace definition has been stored in the rhombus metadata store
		CKeyspaceDefinition updatedRhombusDefinition = cm.hydrateLatestKeyspaceDefinitionFromCassandra(NewKeyspaceDefinition.getName());
		assertEquals(NewKeyspaceDefinition, updatedRhombusDefinition);

		//now query out some data grabbing a brand new object mapper
		cm = getConnectionManager();
		om = cm.getObjectMapper(NewKeyspaceDefinition.getName());

		//now insert some stuff into the newly added object and indexes
		testObject = JsonUtil.rhombusMapFromJsonMap(TestHelpers.getTestObject(0), OldKeyspaceDefinition.getDefinitions().get("testtype"));
		om.insert("testtype", testObject);


		testObject = Maps.newHashMap();
		testObject.put("index_1", "one");
		testObject.put("index_2", "two");
		testObject.put("value", "three");
		key = (UUID)om.insert("simple", testObject);

		//Query to get back the object from the database
		//Query by foreign key
		Criteria criteria = new Criteria();
		SortedMap<String, Object> indexValues = Maps.newTreeMap();
		indexValues.put("data1", "This is data one");
		indexValues.put("data2", "This is data two");
		criteria.setIndexKeys(indexValues);
		List<Map<String, Object>> results = om.list("testtype", criteria);
		assertEquals(777L,results.get(0).get("foreignid"));
		assertEquals("This is data one",results.get(0).get("data1"));

		Map<String,Object> result = om.getByKey("simple", key);
		assertEquals("one",result.get("index_1"));
		assertEquals("two",result.get("index_2"));
		assertEquals("three",result.get("value"));

		// Make sure we have saved both versions of the keyspace
		Session session = cm.getEmptySession();
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT id from \"");
		sb.append(cm.getRhombusKeyspaceName());
		sb.append("\".\"__keyspace_definitions\" where name='");
		sb.append(NewKeyspaceDefinition.getName());
		sb.append("';");
		ResultSet resultSet = session.execute(sb.toString());
		Iterator<Row> rsIter = resultSet.iterator();
		int counter = 0;
		while(rsIter.hasNext()) {
			counter++;
			rsIter.next();
		}
		assertEquals(2, counter);
	}
}
