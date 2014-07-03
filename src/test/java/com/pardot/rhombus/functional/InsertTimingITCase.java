package com.pardot.rhombus.functional;


import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.helpers.TestHelpers;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class InsertTimingITCase extends RhombusFunctionalTest {

	private static Logger logger = LoggerFactory.getLogger(InsertTimingITCase.class);

	@Test
	public void testInsertAsync() throws Exception {
		logger.debug("Starting testInsertAsync");

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "AuditKeyspace.js");
		assertNotNull(definition);

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		logger.debug("Built keyspace: {}", definition.getName());
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setExecuteAsync(true);

		//Insert our test data
		List<Map<String, Object>> values = JsonUtil.rhombusMapFromResource(this.getClass().getClassLoader(), "DateRangeQueryTestData.js");
		for(Map<String, Object> object : values) {
			Long createdAt = (Long)(object.get("created_at"));
			logger.debug("Inserting audit with created_at: {}", createdAt);
			om.insert("object_audit", JsonUtil.rhombusMapFromJsonMap(object,definition.getDefinitions().get("object_audit")), createdAt);
		}

		cm.teardown();
	}

	@Test
	public void testInsertSync() throws Exception {
		logger.debug("Starting testInsertSync");

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "AuditKeyspace.js");
		assertNotNull(definition);

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		logger.debug("Built keyspace: {}", definition.getName());
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setExecuteAsync(false);

		//Insert our test data
		List<Map<String, Object>> values = JsonUtil.rhombusMapFromResource(this.getClass().getClassLoader(), "DateRangeQueryTestData.js");
		for(Map<String, Object> object : values) {
			Long createdAt = (Long)(object.get("created_at"));
			logger.debug("Inserting audit with created_at: {}", createdAt);
			om.insert("object_audit", JsonUtil.rhombusMapFromJsonMap(object,definition.getDefinitions().get("object_audit")), createdAt);
		}

		cm.teardown();
	}

}
