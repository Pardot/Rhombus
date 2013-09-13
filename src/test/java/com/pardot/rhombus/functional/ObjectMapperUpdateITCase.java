package com.pardot.rhombus.functional;


import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.cobject.IndexUpdateRow;
import com.pardot.rhombus.helpers.TestHelpers;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ObjectMapperUpdateITCase extends RhombusFunctionalTest {

	private static Logger logger = LoggerFactory.getLogger(ObjectMapperUpdateITCase.class);


	@Test
	public void testNullIndexValues() throws Exception {
		logger.debug("Starting testNullIndexValues");

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
		om.setLogCql(true);

		//Insert our test data
		List<Map<String, Object>> values = JsonUtil.rhombusMapFromResource(this.getClass().getClassLoader(), "NullIndexValuesTestData.js");
		Map<String, Object> object = values.get(0);
		Long createdAt = (Long)(object.get("created_at"));
		logger.debug("Inserting audit with created_at: {}", createdAt);
		UUID id = om.insert("object_audit", JsonUtil.rhombusMapFromJsonMap(object,definition.getDefinitions().get("object_audit")), createdAt);

		//Get back the data and make sure things match
		Map<String, Object> result = om.getByKey("object_audit", id);

		//Update the object
		Map<String, Object> updates = Maps.newHashMap();
		updates.put("object_id", UUID.fromString("00000003-0000-0030-0040-000000040000"));
		om.update("object_audit", id, updates);


		//Query back the data
		Criteria criteria = new Criteria();
		SortedMap<String, Object> indexKeys = new TreeMap<String, Object>();
		indexKeys.put("account_id", UUID.fromString("00000003-0000-0030-0040-000000030000"));
		indexKeys.put("object_id", UUID.fromString("00000003-0000-0030-0040-000000040000"));
		indexKeys.put("object_type", "Account");
		criteria.setIndexKeys(indexKeys);
		criteria.setOrdering("DESC");
		criteria.setLimit(50l);

		List<Map<String, Object>> dbObjects = om.list("object_audit", criteria);
		assertEquals(1, dbObjects.size());

	}

	@Test
	public void testSendingNullIndexValue() throws Exception {
		logger.debug("Starting testSendingNullIndexValues");

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
		om.setLogCql(true);

		//Insert our test data
		List<Map<String, Object>> values = JsonUtil.rhombusMapFromResource(this.getClass().getClassLoader(), "NullIndexValuesTestData.js");
		Map<String, Object> object = values.get(0);
		Long createdAt = (Long)(object.get("created_at"));
		logger.debug("Inserting audit with created_at: {}", createdAt);
		UUID id = om.insert("object_audit", JsonUtil.rhombusMapFromJsonMap(object,definition.getDefinitions().get("object_audit")), createdAt);

		//Update the object
		object.put("object_id", "00000003-0000-0030-0040-000000040000");
		om.update("object_audit", id, JsonUtil.rhombusMapFromJsonMap(object,definition.getDefinitions().get("object_audit")));

		//Get back the data and make sure things match
		Map<String, Object> result = om.getByKey("object_audit", id);
		assertNotNull(result);
		assertEquals(null, result.get("user_id"));
	}
}
