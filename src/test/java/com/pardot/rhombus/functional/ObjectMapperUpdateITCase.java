package com.pardot.rhombus.functional;


import com.google.common.collect.Maps;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

public class ObjectMapperUpdateITCase extends RhombusFunctionalTest {

	private static Logger logger = LoggerFactory.getLogger(ObjectMapperUpdateITCase.class);


	@Test
	public void simpleTestNullIndexValues() throws Exception {
		String objectType = "simple";
		String index1Value = "value1";
		String index2Value = "value2";

		logger.debug("Starting testNullIndexValues");

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();
		cm.setLogCql(true);

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "SimpleKeyspace.js");
		assertNotNull(definition);

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		logger.debug("Built keyspace: {}", definition.getName());
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setLogCql(true);

		List<Map<String, Object>> values = this.getNValues(1, index1Value, index2Value);
		UUID id = (UUID)om.insert(objectType, values.get(0));

		//Get back the data and make sure things match
		Map<String, Object> result = om.getByKey(objectType, id);
		assertEquals(index1Value, result.get("index_1"));
		assertEquals(index2Value, result.get("index_2"));
		assertEquals("0", result.get("value"));

		//Update the object
		Map<String, Object> updates = Maps.newHashMap();
		updates.put("index_2", null);
		om.update(objectType, id, updates);

		//Get it back by id
		result = om.getByKey(objectType, id);
		assertEquals("0", result.get("value"));
		assertNull(result.get("index_2"));

		//Query back the data by first index
		Criteria criteria = new Criteria();
		SortedMap<String, Object> indexKeys = new TreeMap<String, Object>();
		indexKeys.put("index_1", "value1");
		criteria.setIndexKeys(indexKeys);
		criteria.setOrdering("DESC");
		criteria.setLimit(50l);
		List<Map<String, Object>> dbObjects = om.list(objectType, criteria);
		assertEquals(1, dbObjects.size());
		assertEquals(null, dbObjects.get(0).get("index_2"));

		//Query back the data by second index
		criteria = new Criteria();
		indexKeys = new TreeMap<String, Object>();
		indexKeys.put("index_2", "value2");
		criteria.setIndexKeys(indexKeys);
		criteria.setOrdering("DESC");
		criteria.setLimit(50l);
		dbObjects = om.list(objectType, criteria);
		assertEquals(0, dbObjects.size());
	}

	private List<Map<String, Object>> getNValues(int number, String index1Value, String index2Value) {
		List<Map<String, Object>> values = new ArrayList<Map<String, Object>>();
		for(int i = 0 ; i < number ; i++) {
			Map<String, Object> value = Maps.newHashMap();
			value.put("index_1", index1Value);
			value.put("index_2", index2Value);
			value.put("value", String.valueOf(i));
			values.add(value);
		}
		return values;
	}

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
		UUID id = (UUID)om.insert("object_audit", JsonUtil.rhombusMapFromJsonMap(object,definition.getDefinitions().get("object_audit")), createdAt);

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
		UUID id = (UUID)om.insert("object_audit", JsonUtil.rhombusMapFromJsonMap(object,definition.getDefinitions().get("object_audit")), createdAt);

		//Update the object
		object.put("object_id", "00000003-0000-0030-0040-000000040000");
		om.update("object_audit", id, JsonUtil.rhombusMapFromJsonMap(object,definition.getDefinitions().get("object_audit")));

		//Get back the data and make sure things match
		Map<String, Object> result = om.getByKey("object_audit", id);
		assertNotNull(result);
		assertEquals(null, result.get("user_id"));
	}

	@Test
	public void testUpdatingNullNonIndexValue() throws Exception {
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
		UUID id = (UUID)om.insert("object_audit", JsonUtil.rhombusMapFromJsonMap(object,definition.getDefinitions().get("object_audit")), createdAt);

		//Update the object
		object.put("changes", null);
		om.update("object_audit", id, JsonUtil.rhombusMapFromJsonMap(object,definition.getDefinitions().get("object_audit")));

		//Get back the data and make sure things match
		Map<String, Object> result = om.getByKey("object_audit", id);
		assertNotNull(result);
		assertEquals(null, result.get("changes"));
	}
}
