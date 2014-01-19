package com.pardot.rhombus.functional;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.utils.UUIDs;
import com.pardot.rhombus.CassandraConfiguration;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CField;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.cobject.CQLStatement;
import com.pardot.rhombus.helpers.ConnectionManagerTester;
import com.pardot.rhombus.helpers.TestHelpers;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

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
		ResultSet rs = om.getCqlExecutor().executeSync(CQLStatement.make("SELECT * FROM testtype3cb23c7ffc4256283064bd5eae1886b4"));
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
		session.shutdown();
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
}
