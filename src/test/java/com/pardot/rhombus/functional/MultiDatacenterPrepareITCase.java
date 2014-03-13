package com.pardot.rhombus.functional;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.helpers.TestHelpers;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

public class MultiDatacenterPrepareITCase extends RhombusFunctionalTest {

	private static Logger logger = LoggerFactory.getLogger(MultiDatacenterPrepareITCase.class);

	@Test
	public void testMultiDatacenterInserts() throws Exception {
		logger.debug("Starting testMultiDatacenterInserts");

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "CCMTestData.js");
		assertNotNull(definition);

		//Rebuild the keyspace and get the object mapper
		ObjectMapper om = cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);

		// Do some inserts, making sure that each one will have to prepare a new statement
		List<Map<String, Object>> allInserts = Lists.newArrayList();
		Map<String, Object> toInsert;
		int minField = 0;
		int maxField = 12;
		for(int i = minField ; i <= maxField ; i++) {
			toInsert = Maps.newHashMap();
			toInsert.put("field" + i, "value" + i);
			allInserts.add(toInsert);
			for(int j = maxField ; j > i ; j--) {
				toInsert = Maps.newHashMap();
				toInsert.put("field" + i, "value" + i);
				for(int k = i+1 ; k <= j ; k++) {
					toInsert.put("field" + k, "value" + k);
				}
				allInserts.add(toInsert);
			}
		}

		for(Map<String, Object> insert : allInserts) {
			logger.warn("Inserting {}", insert);
			om.insert("simpletype", insert);
		}

		logger.warn("Successfully inserted {} records where each required a new prepare", allInserts.size());

		//Teardown connections
		cm.teardown();
	}


}
