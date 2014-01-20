package com.pardot.rhombus.helpers;

import com.pardot.rhombus.CassandraConfiguration;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 1/19/14
 */
public class ConnectionManagerTester extends ConnectionManager {

	public ConnectionManagerTester(CassandraConfiguration configuration) {
		super(configuration);
	}

	public ObjectMapper getRhombusObjectMapper(CKeyspaceDefinition templateDefinition) {
		return super.getRhombusObjectMapper(templateDefinition);
	}
}
