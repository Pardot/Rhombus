package com.pardot.rhombus.functional;


import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.RhombusException;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.cobject.CQLGenerationException;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

public class ObjectMapperQueryITCase extends RhombusFunctionalTest {

	private static Logger logger = LoggerFactory.getLogger(ObjectMapperQueryITCase.class);

	String objectType = "object1";
	UUID accountId = UUIDs.random();
	UUID userId = UUIDs.random();

	@Test
	public void testGetLessThanLimitInSingleShard() throws Exception {
		//Build the connection manager
		ConnectionManager cm = getConnectionManager();
		cm.setLogCql(true);

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "ShardedKeyspace.js");

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setLogCql(true);

		long insertNum = 10l;

		insertNObjects(om, insertNum, null);

		// Get 50 back
		long limit = 50l;
		Criteria criteria = new Criteria();
		SortedMap<String, Object> indexKeys = Maps.newTreeMap();
		indexKeys.put("account_id", accountId);
		indexKeys.put("user_id", userId);
		criteria.setIndexKeys(indexKeys);
		criteria.setLimit(limit);
		List<Map<String, Object>> results = om.list(objectType, criteria);

		assertEquals(insertNum, results.size());

		cm.teardown();
	}

	@Test
	public void testGetInEmptyShardList() throws Exception {
		//Build the connection manager
		ConnectionManager cm = getConnectionManager();
		cm.setLogCql(true);

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "ShardedKeyspace.js");

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setLogCql(true);

		// Get 50 back
		long limit = 50l;
		Criteria criteria = new Criteria();
		SortedMap<String, Object> indexKeys = Maps.newTreeMap();
		indexKeys.put("account_id", accountId);
		indexKeys.put("user_id", userId);
		criteria.setIndexKeys(indexKeys);
		criteria.setLimit(limit);
		List<Map<String, Object>> results = om.list(objectType, criteria);

		assertEquals(0, results.size());

		cm.teardown();
	}

	@Test
	public void testInsertWithIdsAndGet() throws Exception {
		//Build the connection manager
		ConnectionManager cm = getConnectionManager();
		cm.setLogCql(true);

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "ShardedKeyspace.js");

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setLogCql(true);

		long insertNum = 18l;

		List<UUID> idList = Lists.newArrayList();
		for(int i = 0 ; i < insertNum ; i++) {
			idList.add(UUIDs.timeBased());
		}

		insertNObjects(om, insertNum, idList);

		// Get 50 back
		long limit = 50l;
		Criteria criteria = new Criteria();
		SortedMap<String, Object> indexKeys = Maps.newTreeMap();
		indexKeys.put("account_id", accountId);
		indexKeys.put("user_id", userId);
		criteria.setIndexKeys(indexKeys);
		criteria.setLimit(limit);
		List<Map<String, Object>> results = om.list(objectType, criteria);

		assertEquals(insertNum, results.size());

		for(UUID id : idList) {
			assertTrue(resultsContainId(results, id));
		}

		cm.teardown();
	}

	@Test
	public void testInsert2WithIdsAcrossShardsAndGet() throws Exception {
		//Build the connection manager
		ConnectionManager cm = getConnectionManager();
		cm.setLogCql(true);

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "ShardedKeyspace.js");

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setLogCql(true);

		long insertNum = 2l;

		List<UUID> idList = Lists.newArrayList();
		idList.add(UUID.fromString("7dcf8c00-bcc1-11e2-8080-808080808080"));
		idList.add(UUID.fromString("6d554c00-61ce-11e2-8080-808080808080"));

		insertNObjects(om, insertNum, idList);

		// Get 50 back
		long limit = 50l;
		Criteria criteria = new Criteria();
		SortedMap<String, Object> indexKeys = Maps.newTreeMap();
		indexKeys.put("account_id", accountId);
		indexKeys.put("user_id", userId);
		criteria.setIndexKeys(indexKeys);
		criteria.setLimit(limit);
		List<Map<String, Object>> results = om.list(objectType, criteria);

		assertEquals(insertNum, results.size());

		for(UUID id : idList) {
			assertTrue(resultsContainId(results, id));
		}

		cm.teardown();
	}

	@Test
	public void testInsert18WithIdsAcrossShardsAndGet() throws Exception {
		//Build the connection manager
		ConnectionManager cm = getConnectionManager();
		cm.setLogCql(true);

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "ShardedKeyspace.js");

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setLogCql(true);

		long insertNum = 18l;

		List<UUID> idList = Lists.newArrayList();
		idList.add(UUID.fromString("7dcf8c00-bcc1-11e2-8080-808080808080"));
		idList.add(UUID.fromString("d1db7000-bcc3-11e2-8080-808080808080"));
		idList.add(UUID.fromString("25e75400-bcc6-11e2-8080-808080808080"));
		idList.add(UUID.fromString("79f33800-bcc8-11e2-8080-808080808080"));
		idList.add(UUID.fromString("cdff1c00-bcca-11e2-8080-808080808080"));
		idList.add(UUID.fromString("220b0000-bccd-11e2-8080-808080808080"));
		idList.add(UUID.fromString("7616e400-bccf-11e2-8080-808080808080"));
		idList.add(UUID.fromString("ca22c800-bcd1-11e2-8080-808080808080"));
		idList.add(UUID.fromString("1e2eac00-bcd4-11e2-8080-808080808080"));
		idList.add(UUID.fromString("723a9000-bcd6-11e2-8080-808080808080"));
		idList.add(UUID.fromString("c6467400-bcd8-11e2-8080-808080808080"));
		idList.add(UUID.fromString("f1a16400-bbef-11e2-8080-808080808080"));
		idList.add(UUID.fromString("48574400-ba1e-11e2-8080-808080808080"));
		idList.add(UUID.fromString("5cff6400-9ed5-11e2-8080-808080808080"));
		idList.add(UUID.fromString("3ffb0000-a496-11e2-8080-808080808080"));
		idList.add(UUID.fromString("1d2cc000-8c3a-11e2-8080-808080808080"));
		idList.add(UUID.fromString("799bc000-7639-11e2-8080-808080808080"));
		idList.add(UUID.fromString("6d554c00-61ce-11e2-8080-808080808080"));

		insertNObjects(om, insertNum, idList);

		// Get 50 back
		long limit = 50l;
		Criteria criteria = new Criteria();
		SortedMap<String, Object> indexKeys = Maps.newTreeMap();
		indexKeys.put("account_id", accountId);
		indexKeys.put("user_id", userId);
		criteria.setIndexKeys(indexKeys);
		criteria.setLimit(limit);
		List<Map<String, Object>> results = om.list(objectType, criteria);

		assertEquals(insertNum, results.size());

		for(UUID id : idList) {
			assertTrue(resultsContainId(results, id));
		}

		cm.teardown();
	}

	@Test
	public void testInsert18WithIdsAcrossShardsAndCount() throws Exception {
		//Build the connection manager
		ConnectionManager cm = getConnectionManager();
		cm.setLogCql(true);

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "ShardedKeyspace.js");

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setLogCql(true);

		long insertNum = 18l;

		List<UUID> idList = Lists.newArrayList();
		idList.add(UUID.fromString("7dcf8c00-bcc1-11e2-8080-808080808080"));
		idList.add(UUID.fromString("d1db7000-bcc3-11e2-8080-808080808080"));
		idList.add(UUID.fromString("25e75400-bcc6-11e2-8080-808080808080"));
		idList.add(UUID.fromString("79f33800-bcc8-11e2-8080-808080808080"));
		idList.add(UUID.fromString("cdff1c00-bcca-11e2-8080-808080808080"));
		idList.add(UUID.fromString("220b0000-bccd-11e2-8080-808080808080"));
		idList.add(UUID.fromString("7616e400-bccf-11e2-8080-808080808080"));
		idList.add(UUID.fromString("ca22c800-bcd1-11e2-8080-808080808080"));
		idList.add(UUID.fromString("1e2eac00-bcd4-11e2-8080-808080808080"));
		idList.add(UUID.fromString("723a9000-bcd6-11e2-8080-808080808080"));
		idList.add(UUID.fromString("c6467400-bcd8-11e2-8080-808080808080"));
		idList.add(UUID.fromString("f1a16400-bbef-11e2-8080-808080808080"));
		idList.add(UUID.fromString("48574400-ba1e-11e2-8080-808080808080"));
		idList.add(UUID.fromString("5cff6400-9ed5-11e2-8080-808080808080"));
		idList.add(UUID.fromString("3ffb0000-a496-11e2-8080-808080808080"));
		idList.add(UUID.fromString("1d2cc000-8c3a-11e2-8080-808080808080"));
		idList.add(UUID.fromString("799bc000-7639-11e2-8080-808080808080"));
		idList.add(UUID.fromString("6d554c00-61ce-11e2-8080-808080808080"));

		insertNObjects(om, insertNum, idList);

		Criteria criteria = new Criteria();
		SortedMap<String, Object> indexKeys = Maps.newTreeMap();
		indexKeys.put("account_id", accountId);
		indexKeys.put("user_id", userId);
		criteria.setIndexKeys(indexKeys);

		long count = om.count(objectType, criteria);
		assertEquals(insertNum, count);

		cm.teardown();
	}

	@Test
	public void testInsert18WithIdsAcrossShardsAndCountWithFiltering() throws Exception {
		//Build the connection manager
		ConnectionManager cm = getConnectionManager();
		cm.setLogCql(true);

		//Build our keyspace definition object
		CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "ShardedKeyspace.js");

		//Rebuild the keyspace and get the object mapper
		cm.buildKeyspace(definition, true);
		cm.setDefaultKeyspace(definition);
		ObjectMapper om = cm.getObjectMapper();
		om.setLogCql(true);

		long insertNum = 18l;

		List<UUID> idList = Lists.newArrayList();
		idList.add(UUID.fromString("7dcf8c00-bcc1-11e2-8080-808080808080"));
		idList.add(UUID.fromString("d1db7000-bcc3-11e2-8080-808080808080"));
		idList.add(UUID.fromString("25e75400-bcc6-11e2-8080-808080808080"));
		idList.add(UUID.fromString("79f33800-bcc8-11e2-8080-808080808080"));
		idList.add(UUID.fromString("cdff1c00-bcca-11e2-8080-808080808080"));
		idList.add(UUID.fromString("220b0000-bccd-11e2-8080-808080808080"));
		idList.add(UUID.fromString("7616e400-bccf-11e2-8080-808080808080"));
		idList.add(UUID.fromString("ca22c800-bcd1-11e2-8080-808080808080"));
		idList.add(UUID.fromString("1e2eac00-bcd4-11e2-8080-808080808080"));
		idList.add(UUID.fromString("723a9000-bcd6-11e2-8080-808080808080"));
		idList.add(UUID.fromString("c6467400-bcd8-11e2-8080-808080808080"));
		idList.add(UUID.fromString("f1a16400-bbef-11e2-8080-808080808080"));
		idList.add(UUID.fromString("48574400-ba1e-11e2-8080-808080808080"));
		idList.add(UUID.fromString("5cff6400-9ed5-11e2-8080-808080808080"));
		idList.add(UUID.fromString("3ffb0000-a496-11e2-8080-808080808080"));
		idList.add(UUID.fromString("1d2cc000-8c3a-11e2-8080-808080808080"));
		idList.add(UUID.fromString("799bc000-7639-11e2-8080-808080808080"));
		idList.add(UUID.fromString("6d554c00-61ce-11e2-8080-808080808080"));

		insertNObjects(om, insertNum, idList);

		Map<String, Object> object = Maps.newHashMap();
		object.put("id", UUIDs.timeBased());
		object.put("account_id", accountId);
		object.put("user_id", UUIDs.random());
		om.insert(objectType, object);

		Criteria criteria = new Criteria();
		SortedMap<String, Object> indexKeys = Maps.newTreeMap();
		indexKeys.put("account_id", accountId);
		indexKeys.put("user_id", userId);
		criteria.setIndexKeys(indexKeys);

		long count = om.count(objectType, criteria);
		assertEquals(insertNum, count);

		cm.teardown();
	}

	private boolean resultsContainId(List<Map<String, Object>> results, UUID id) {
		for(Map<String, Object> object : results) {
			if(id.equals(object.get("id"))) {
				return true;
			}
		}
		return false;
	}

	public void insertNObjects(ObjectMapper om, long number, List<UUID> idList) throws RhombusException, CQLGenerationException {
		List<Map<String, Object>> objects = Lists.newArrayList();
		for(int i = 0 ; i < number ; i++) {
			Map<String, Object> value = Maps.newHashMap();
			if(idList != null) {
				value.put("id", idList.get(i));
			}
			value.put("account_id", accountId);
			value.put("user_id", userId);
			objects.add(value);
		}
		Map<String, List<Map<String, Object>>> batch = Maps.newHashMap();
		batch.put(objectType, objects);
		om.insertBatchMixed(batch);
	}
}
