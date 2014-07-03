package com.pardot.rhombus.functional;


import com.datastax.driver.core.utils.UUIDs;
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

public class ObjectMapperTableScanITCase extends RhombusFunctionalTest {

	private static Logger logger = LoggerFactory.getLogger(ObjectMapperTableScanITCase.class);


	@Test
	public void testBasicScan() throws Exception {
		String objectType = "simple";
		String index1Value = "value1";
		String index2Value = "value2";

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();
		cm.setLogCql(true);

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "SimpleKeyspace.js");

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		logger.debug("Built keyspace: {}", definition.getName());
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setLogCql(true);

		// Insert 20 values
		List<Map<String, Object>> values = this.getNValues(20, index1Value, index2Value);
		for(Map<String, Object> insertValue : values) {
			om.insert(objectType, insertValue);
		}

		// Get back the first 10
		List<Map<String, Object>> first10 = om.scanTableWithStartToken(objectType, Long.MIN_VALUE, Long.MAX_VALUE, 10l);
		assertEquals(10, first10.size());

		// Get back the rest, starting with the last id in the first set
		List<Map<String, Object>> next10 = om.scanTableWithStartId(objectType, String.valueOf(first10.get(first10.size()-1).get("id")), Long.MAX_VALUE, 100l);
		assertEquals(10, next10.size());
		for(Map<String, Object> value : next10) {
			assertFalse(valuesContainsId(first10, value.get("id")));
		}

		cm.teardown();
	}

	private boolean valuesContainsId(List<Map<String, Object>> values, Object id) {
		for(Map<String, Object> object : values) {
			if(id.equals(object.get("id"))) {
				return true;
			}
		}
		return false;
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
}
