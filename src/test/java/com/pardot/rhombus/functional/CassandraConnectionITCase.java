package com.pardot.rhombus.functional;

import static org.junit.Assert.*;

import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.pardot.rhombus.helpers.TestHelpers;
import org.junit.Test;

import com.datastax.driver.core.Session;

import com.pardot.rhombus.ConnectionManager;

import java.io.IOException;

public class CassandraConnectionITCase extends RhombusFunctionalTest {

	@Test
	public void testKeyspaceCreate() throws IOException {
		ConnectionManager cm = TestHelpers.getTestConnectionManager();
		cm.buildCluster();
		Session session = cm.getEmptySession();
		assertNotNull(session);

		//Drop the functional keyspace if it exists
		try {
			session.execute("DROP KEYSPACE functional_create");
		} catch (InvalidQueryException e) {
			//Ignore
		}

		//Create the functional keyspace
		session.execute("CREATE KEYSPACE functional_create WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

		//Change to our functional testing keyspace
		session.execute("USE functional_create");

		//Drop the functional keyspace
		try {
			session.execute("DROP KEYSPACE functional_create");
		} catch (InvalidQueryException e) {
			//Ignore
		}

		//Shutdown the session
		session.close();

		//Teardown the connection manager
		cm.teardown();
	}


}
