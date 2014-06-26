package com.pardot.rhombus.functional;


import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Maps;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.util.JsonUtil;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

public class ObjectMapperShardingITCase extends RhombusFunctionalTest {

	private static Logger logger = LoggerFactory.getLogger(ObjectMapperShardingITCase.class);

	private static CKeyspaceDefinition keyspaceDefinition;

	@BeforeClass
	public static void setupKeyspace()
	{
		try {
			ConnectionManager cm = getConnectionManagerStatic();
			keyspaceDefinition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, ObjectMapperShardingITCase.class.getClassLoader(), "ShardedKeyspace.js");
			cm.buildKeyspace(keyspaceDefinition, true);
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void testShardedQueries() throws Exception {
		logger.debug("Starting testShardedQueries");

		// Get an object mapper for the keyspace and truncate the data
		ConnectionManager cm = getConnectionManager();
		ObjectMapper om = cm.getObjectMapper(keyspaceDefinition);
		om.truncateTables();
		om.setLogCql(true);

		//Set up test data
		List<Map<String, Object>> values = JsonUtil.rhombusMapFromResource(this.getClass().getClassLoader(), "ShardedTestData.js");
		for(Map<String, Object> object : values) {
			Map<String, Object> updatedObject = JsonUtil.rhombusMapFromJsonMap(object, keyspaceDefinition.getDefinitions().get("object1"));
			Long createdAt = ((Date)(updatedObject.get("created_at"))).getTime();
			logger.debug("Inserting object with created_at: {}", createdAt);
			UUID id = (UUID)om.insert("object1", updatedObject, createdAt);
			logger.debug("Inserted object with uuid unix time: {}", UUIDs.unixTimestamp(id));
		}

		//Query it back out
		//Make sure that we have the proper number of results
		SortedMap<String, Object> indexValues = Maps.newTreeMap();
		indexValues.put("account_id", UUID.fromString("00000003-0000-0030-0040-000000030000"));
		indexValues.put("user_id", UUID.fromString("00000003-0000-0030-0040-000000030000"));
		Criteria criteria = new Criteria();
		criteria.setIndexKeys(indexValues);
		criteria.setLimit(50L);
		List<Map<String, Object>> results = om.list("object1", criteria);
		assertEquals(3, results.size());
	}

	@Test
	public void testQueriesUseShardIndex() throws Exception {
		logger.debug("Starting testQueriesUseShardIndex");

		// Get an object mapper for the keyspace and truncate the data
		ConnectionManager cm = getConnectionManager();
		ObjectMapper om = cm.getObjectMapper(keyspaceDefinition);
		om.truncateTables();
		om.setLogCql(true);

		// Insert a record that is more than ObjectMapper.reasonableStatementLimit months old
		DateTime dateTime = DateTime.now().minusMonths(om.getReasonableStatementLimit() * 2);
		UUID id = UUIDs.startOf(dateTime.getMillis());
		UUID accountId = UUID.fromString("00000003-0000-0030-0040-000000030000");
		UUID userId = UUID.fromString("00000003-0000-0030-0040-000000030000");
		Map<String, Object> record = Maps.newHashMap();
		record.put("id", id);
		record.put("account_id", accountId);
		record.put("user_id", userId);
		om.insert("object1", record);

		//Query it back out, make sure that we are using the shard index, because if we are not, we will surpass reasonable statement limit
		SortedMap<String, Object> indexValues = Maps.newTreeMap();
		indexValues.put("account_id", accountId);
		indexValues.put("user_id", userId);
		Criteria criteria = new Criteria();
		criteria.setIndexKeys(indexValues);
		criteria.setEndTimestamp(DateTime.now().getMillis());
		criteria.setLimit(50L);
		List<Map<String, Object>> results = om.list("object1", criteria);

		assertEquals(1, results.size());
	}

}
