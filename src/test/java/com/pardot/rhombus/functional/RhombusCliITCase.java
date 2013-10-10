package com.pardot.rhombus.functional;

import com.google.common.collect.Lists;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cli.RhombusCli;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * User: Rob Righter
 * Date: 10/10/13
 */
public class RhombusCliITCase extends RhombusFunctionalTest {

	public String getWorkingPath(){
		return RhombusCliITCase.class.getClassLoader().getResource("").getPath().toString();
	}

	@Test
	public void testRebuildKeyspaceCLI() throws Exception {

		String workingpath = getWorkingPath();

		RhombusCli.runit((String[]) Arrays.asList(
				"RebuildKeyspace",
				"-cassconfig", workingpath + "cassandra-functional.js",
				"-keyspacefile", workingpath + "cli-functional-keyspacedef.js",
				"-f"
		).toArray());

		//now verify that the keyspace was created and is working
		ConnectionManager cm = getConnectionManager();
		ObjectMapper om = cm.getObjectMapper("clifunctional");
		CKeyspaceDefinition keyspace = om.getKeyspaceDefinition_ONLY_FOR_TESTING();
		assertEquals("clifunctional", keyspace.getName());
		assertEquals(3, keyspace.getDefinitions().get("clitest").getFields().size());
		assertEquals(2, keyspace.getDefinitions().get("clitest").getIndexes().size());
	}

	@Test
	public void testRunMigrationCLI() throws Exception {

		String workingpath = getWorkingPath();

		assertTrue(RhombusCli.runit((String[])Arrays.asList(
				"RebuildKeyspace",
				"-cassconfig", workingpath+"cassandra-functional.js",
				"-keyspacefile", workingpath+"cli-functional-keyspacedef.js",
				"-f"
		).toArray()));

		//now verify that the keyspace was created and is working
		ConnectionManager cm = getConnectionManager();
		ObjectMapper om = cm.getObjectMapper("clifunctional");
		CKeyspaceDefinition keyspace = om.getKeyspaceDefinition_ONLY_FOR_TESTING();
		assertEquals("clifunctional", keyspace.getName());
		assertEquals(3, keyspace.getDefinitions().get("clitest").getFields().size());
		assertEquals(2, keyspace.getDefinitions().get("clitest").getIndexes().size());


		//now migrate the keyspace
		assertTrue(RhombusCli.runit((String[])Arrays.asList(
				"RunMigration",
				"-cassconfig", workingpath+"cassandra-functional.js",
				"-newkeyspacefile", workingpath+"cli-functional-keyspacedef-migration.js",
				"-keyspace", "clifunctional"
		).toArray()));

		//now verify that the keyspace was migrated and is working
		cm = getConnectionManager();
		om = cm.getObjectMapper("clifunctional");
		keyspace = om.getKeyspaceDefinition_ONLY_FOR_TESTING();
		assertEquals("clifunctional", keyspace.getName());
		assertEquals(3, keyspace.getDefinitions().get("clitest").getFields().size());
		assertEquals(3, keyspace.getDefinitions().get("clitest").getIndexes().size());
		assertEquals(2, keyspace.getDefinitions().get("clitest2").getIndexes().size());
	}


}
