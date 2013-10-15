package com.pardot.rhombus.functional;

import com.datastax.driver.core.Cluster;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.helpers.TestHelpers;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

/**
 * User: Rob Righter
 * Date: 8/20/13
 */
public abstract class RhombusFunctionalTest {

    protected ConnectionManager getConnectionManager() throws IOException {
        //Get a connection manager based on the test properties
        ConnectionManager connectionManager = TestHelpers.getTestConnectionManager();
        connectionManager.setLogCql(true);
        connectionManager.buildCluster(true);
        assertNotNull(connectionManager);
        return connectionManager;
    }
}
