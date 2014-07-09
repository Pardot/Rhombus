package com.pardot.rhombus.functional;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.*;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.cobject.CObjectTokenVisitor;
import com.pardot.rhombus.cobject.CObjectTokenVisitorFactory;
import com.pardot.rhombus.cobject.CQLGenerationException;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;

public class QueryPerformanceITCase extends RhombusFunctionalTest {

	private static Logger logger = LoggerFactory.getLogger(QueryPerformanceITCase.class);



	@Test
	public void testGet50Of2000() throws Exception {
		String objectType = "simple";

		//Build the connection manager
		ConnectionManager cm = getConnectionManager();
		cm.setLogCql(true);

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "SimpleKeyspace.js");

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setLogCql(true);


		long insertNum = 1000l;
		long batchSize = 200;
		insertNObjects(om, insertNum, batchSize);

		// Get 50 back
		long limit = 50l;
		Criteria criteria = new Criteria();
		SortedMap<String, Object> indexKeys = Maps.newTreeMap();
		indexKeys.put("index_1", "value1");
		criteria.setIndexKeys(indexKeys);
		criteria.setLimit(limit);
		List<Map<String, Object>> results = om.list(objectType, criteria);

		assertEquals(limit, results.size());

		cm.teardown();
	}

	public void insertNObjects(ObjectMapper om, long number, long batchSize) throws RhombusException, CQLGenerationException {
		if(number < batchSize) {
			batchSize = number;
		}
		long numInserted = 0;
		while(numInserted < number) {
			long numLeft = number - numInserted;
			long numToInsert = batchSize;
			if(numLeft < batchSize) {
				numToInsert = numLeft;
			}
			List<Map<String, Object>> objects = getNValues(numToInsert, "value1", "value2");
			Map<String, List<Map<String, Object>>> batch = Maps.newHashMap();
			batch.put("simple", objects);
			om.insertBatchMixed(batch);
			numInserted += numToInsert;
		}
	}

	private List<Map<String, Object>> getNValues(long number, String index1Value, String index2Value) {
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
