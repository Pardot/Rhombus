package com.pardot.rhombus.functional;


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.utils.UUIDs;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.CField;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.cobject.CQLStatement;
import com.pardot.rhombus.helpers.TestHelpers;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConnectionManagerITCase extends RhombusFunctionalTest {

	private static Logger logger = LoggerFactory.getLogger(ConnectionManagerITCase.class);

	@Test
	public void testBuildKeyspace() throws Exception {
		logger.debug("testBuildKeyspace");
		// Set up a connection manager and build the cluster
		ConnectionManager cm = getConnectionManager();

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class
				, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		assertNotNull(definition);

		//Build the keyspace forcing a rebuild in case anything has been left behind
		cm.buildKeyspace(definition, true);

		//Create a table and insert a row
		ObjectMapper om = cm.getObjectMapper(definition.getName());
		UUID uuid = UUIDs.timeBased();
		om.getCqlExecutor().executeSync(CQLStatement.make("CREATE TABLE cmit (id UUID PRIMARY KEY);"));
		logger.debug("Created table");
		om.getCqlExecutor().executeSync(CQLStatement.make("INSERT INTO cmit (id) VALUES (?);", Arrays.asList(uuid).toArray()) );
		logger.debug("Inserted");

		//Build the same keyspace but do not force a rebuild
		cm.buildKeyspace(definition, false);

		//Make sure that we can get back the value we inserted
		ResultSet rs = om.getCqlExecutor().executeSync(CQLStatement.make("SELECT * FROM cmit WHERE id=?;",Arrays.asList(uuid).toArray()));
		assertEquals(1, rs.all().size());

		//verify that we saved the keyspace definition in cassandra correctly
		cm = getConnectionManager();
		CKeyspaceDefinition queriedDef = cm.getObjectMapper(definition.getName()).getKeyspaceDefinition_ONLY_FOR_TESTING();
		assertEquals(definition.getDefinitions().size(), queriedDef.getDefinitions().size());
		assertEquals(queriedDef.getDefinitions().get("testtype").getField("foreignid").getType(), CField.CDataType.BIGINT);
	}

	@Test
	public void testBuildPiKeyspace() throws Exception {
		logger.debug("testBuildKeyspace");
		// Set up a connection manager and build the cluster
		ConnectionManager cm = getConnectionManager();

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class
				, this.getClass().getClassLoader(), "pikeyspace-functional.js");
		assertNotNull(definition);

		//Build the keyspace forcing a rebuild in case anything has been left behind
		cm.buildKeyspace(definition, true);

	}

	@Test(expected=InvalidQueryException.class)
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
		ObjectMapper om = cm.getObjectMapper(definition2.getName());
		ResultSet rs = om.getCqlExecutor().executeSync(CQLStatement.make("SELECT * FROM testtype3cb23c7ffc4256283064bd5eae1886b4"));
		assertEquals(0, rs.all().size());

		//Build the keyspace again, but force a rebuild
		cm.buildKeyspace(definition, true);

		//Make sure that the additional index table no longer exists
		om = cm.getObjectMapper(definition.getName());
		om.getCqlExecutor().executeSync(CQLStatement.make("SELECT * FROM testtype3cb23c7ffc4256283064bd5eae1886b4"));

		//Make sure that we saved the keyspace definition
		om = cm.getObjectMapper(definition.getName());
	}
}
