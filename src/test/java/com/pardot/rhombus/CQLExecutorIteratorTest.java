package com.pardot.rhombus;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.ConnectionManager;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.*;
import com.pardot.rhombus.cobject.statement.CQLExecutorIterator;
import com.pardot.rhombus.cobject.statement.CQLStatementIterator;
import com.pardot.rhombus.cobject.statement.UnboundableCQLStatementIterator;
import com.pardot.rhombus.cobject.statement.BaseCQLStatementIterator;
import com.pardot.rhombus.helpers.ConnectionManagerTester;
import com.pardot.rhombus.helpers.TestHelpers;
import com.pardot.rhombus.util.JsonUtil;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**
 * Created by himanshu.pagey on 5/15/14.
 */
public class CQLExecutorIteratorTest  extends TestCase {

	static final String KEYSPACE_NAME = "pifunctional";
	static final String TABLE_NAME = "pifunctional";

	public class ShardListMock implements CObjectShardList {
		List<Long> result;
		public ShardListMock(List<Long> result){
			this.result = result;
		}

		@Override
		public List<Long> getShardIdList(CDefinition def, SortedMap<String, Object> indexValues, CObjectOrdering ordering, @Nullable UUID start, @Nullable UUID end) {
			return result;
		}
	}

	public class Subject extends CObjectCQLGenerator {

		public List<String> uuidList = null;

		public Subject(Integer consistencyHorizon){
			super(KEYSPACE_NAME, consistencyHorizon);
		}

		private List<Map<String, Object>> generateNObjects(int nObjects){

			String timeBaseUuidStr = UUIDs.startOf(System.currentTimeMillis()).toString();
			String uuidPart1 = timeBaseUuidStr.substring(0, 8);
			String uuidPart2 = timeBaseUuidStr.substring(8, timeBaseUuidStr.length());
			long uuidPart1Int = Long.parseLong(uuidPart1, 16);
			List<Map<String, Object>> values2 = Lists.newArrayList();
			uuidList = Lists.newArrayList();

			for (int i = 0; i < nObjects; i++) {
				String uuid = String.format("%08x", (uuidPart1Int)) + uuidPart2;
				Map<String, Object> value = Maps.newHashMap();
				value.put("id", uuid);
				value.put("account_id", "00000003-0000-0030-0040-000000030000");
				value.put("user_id", "00000003-0000-0030-0040-000000030000");
				// Set a specific value for data we want to filter
				if (i % 3 == 0) {
					value.put("field2", "taco");
				} else {
					value.put("field2", "Value" + (i + 8));
				}
				values2.add(value);
				uuidList.add(uuid);
				uuidPart1Int++;

			}

			return values2;
		}

		public void testOneObject() throws Exception {

			//Get a connection manager based on the test properties
			ConnectionManagerTester cm = TestHelpers.getTestConnectionManager();
			cm.setLogCql(true);
			cm.buildCluster(true);

			CObjectShardList shardIdLists = new ShardListMock(Arrays.asList(1L,2L,3L,4L,5L));

			//Build our keyspace definition object
			CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "MultiInsertKeyspace.js");

			//Rebuild the keyspace and get the object mapper
			cm.buildKeyspace(definition, true);

			ObjectMapper om = cm.getObjectMapper(definition);
			om.setLogCql(true);

			int nDataItems = 1;

			List<Map<String, Object>> values2 = generateNObjects(nDataItems);

			List<Map<String, Object>> updatedValues2 = Lists.newArrayList();
			for (Map<String, Object> baseValue : values2) {
				updatedValues2.add(JsonUtil.rhombusMapFromJsonMap(baseValue, definition.getDefinitions().get("object2")));
			}

			Map<String, List<Map<String, Object>>> multiInsertMap = Maps.newHashMap();
			multiInsertMap.put("object2", updatedValues2);

			//Insert data
			om.insertBatchMixed(multiInsertMap);

			// generate a executorIterator
			SortedMap<String, Object> indexValues = Maps.newTreeMap();
			indexValues.put("account_id", UUID.fromString("00000003-0000-0030-0040-000000030000"));
			indexValues.put("user_id", UUID.fromString("00000003-0000-0030-0040-000000030000"));

			UUID stop = UUID.fromString(uuidList.get(nDataItems-1));
			CDefinition cDefinition = definition.getDefinitions().get("object2");
			BaseCQLStatementIterator unBoundedIterator = (BaseCQLStatementIterator) CObjectCQLGenerator.makeCQLforList(KEYSPACE_NAME, shardIdLists, cDefinition, indexValues, CObjectOrdering.DESCENDING, null, stop, 10l, true, false, false);
			Session session = cm.getRhombusSession(definition);
			CQLExecutor cqlExecutor = new CQLExecutor(session, true, definition.getConsistencyLevel());
			CQLExecutorIterator cqlExecutorIterator = new CQLExecutorIterator(cqlExecutor, unBoundedIterator);
			cqlExecutorIterator.setPageSize(nDataItems);


			assertTrue(cqlExecutorIterator.hasNext());
			assertNotNull(cqlExecutorIterator.next());
			assertFalse(cqlExecutorIterator.hasNext());
			assertNull(cqlExecutorIterator.next());

		}

		public void testIterator() throws Exception {

			//Get a connection manager based on the test properties
			ConnectionManagerTester cm = TestHelpers.getTestConnectionManager();
			cm.setLogCql(true);
			cm.buildCluster(true);

			CObjectShardList shardIdLists = new ShardListMock(Arrays.asList(1L,2L,3L,4L,5L));

			//Build our keyspace definition object
			CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "MultiInsertKeyspace.js");

			//Rebuild the keyspace and get the object mapper
			cm.buildKeyspace(definition, true);

			ObjectMapper om = cm.getObjectMapper(definition);
			om.setLogCql(true);

			// Set up test data
			// we will insert 200 objects
			int nDataItems = 200;

			List<Map<String, Object>> values2 = generateNObjects(nDataItems);

			List<Map<String, Object>> updatedValues2 = Lists.newArrayList();
			for (Map<String, Object> baseValue : values2) {
				updatedValues2.add(JsonUtil.rhombusMapFromJsonMap(baseValue, definition.getDefinitions().get("object2")));
			}

			Map<String, List<Map<String, Object>>> multiInsertMap = Maps.newHashMap();
			multiInsertMap.put("object2", updatedValues2);

			//Insert data
			om.insertBatchMixed(multiInsertMap);

			// generate a executorIterator
			SortedMap<String, Object> indexValues = Maps.newTreeMap();
			indexValues.put("account_id", UUID.fromString("00000003-0000-0030-0040-000000030000"));
			indexValues.put("user_id", UUID.fromString("00000003-0000-0030-0040-000000030000"));

			UUID stop = UUID.fromString(uuidList.get(nDataItems-1));
			CDefinition cDefinition = definition.getDefinitions().get("object2");
			BaseCQLStatementIterator unBoundedIterator = (BaseCQLStatementIterator) CObjectCQLGenerator.makeCQLforList(KEYSPACE_NAME, shardIdLists, cDefinition, indexValues, CObjectOrdering.DESCENDING, null, stop, 10l, true, false, false);
			Session session = cm.getRhombusSession(definition);
			CQLExecutor cqlExecutor = new CQLExecutor(session, true, definition.getConsistencyLevel());
			CQLExecutorIterator cqlExecutorIterator = new CQLExecutorIterator(cqlExecutor, unBoundedIterator);
			cqlExecutorIterator.setPageSize(nDataItems);


			for (int i=0 ; i< nDataItems; i++ ){
				assertTrue(cqlExecutorIterator.hasNext());
				assertNotNull(cqlExecutorIterator.next());
			}

			assertFalse(cqlExecutorIterator.hasNext());
		}

		public void testPageSizePlus1() throws Exception {

			//Get a connection manager based on the test properties
			ConnectionManagerTester cm = TestHelpers.getTestConnectionManager();
			cm.setLogCql(true);
			cm.buildCluster(true);

			CObjectShardList shardIdLists = new ShardListMock(Arrays.asList(1L,2L,3L,4L,5L));

			//Build our keyspace definition object
			CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "MultiInsertKeyspace.js");

			//Rebuild the keyspace and get the object mapper
			cm.buildKeyspace(definition, true);

			ObjectMapper om = cm.getObjectMapper(definition);
			om.setLogCql(true);

			// Set up test data
			// we will insert 200 objects
			int nDataItems = 201;

			List<Map<String, Object>> values2 = generateNObjects(nDataItems);

			List<Map<String, Object>> updatedValues2 = Lists.newArrayList();
			for (Map<String, Object> baseValue : values2) {
				updatedValues2.add(JsonUtil.rhombusMapFromJsonMap(baseValue, definition.getDefinitions().get("object2")));
			}

			Map<String, List<Map<String, Object>>> multiInsertMap = Maps.newHashMap();
			multiInsertMap.put("object2", updatedValues2);

			//Insert data
			om.insertBatchMixed(multiInsertMap);

			// generate a executorIterator
			SortedMap<String, Object> indexValues = Maps.newTreeMap();
			indexValues.put("account_id", UUID.fromString("00000003-0000-0030-0040-000000030000"));
			indexValues.put("user_id", UUID.fromString("00000003-0000-0030-0040-000000030000"));

			UUID stop = UUID.fromString(uuidList.get(nDataItems-1));
			CDefinition cDefinition = definition.getDefinitions().get("object2");
			BaseCQLStatementIterator unBoundedIterator = (BaseCQLStatementIterator) CObjectCQLGenerator.makeCQLforList(KEYSPACE_NAME, shardIdLists, cDefinition, indexValues, CObjectOrdering.DESCENDING, null, stop, 10l, true, false, false);
			Session session = cm.getRhombusSession(definition);
			CQLExecutor cqlExecutor = new CQLExecutor(session, true, definition.getConsistencyLevel());
			CQLExecutorIterator cqlExecutorIterator = new CQLExecutorIterator(cqlExecutor, unBoundedIterator);
			cqlExecutorIterator.setPageSize(nDataItems-1);


			for (int i=0 ; i< nDataItems; i++ ){
				assertTrue(cqlExecutorIterator.hasNext());
				assertNotNull(cqlExecutorIterator.next());
			}

			assertFalse(cqlExecutorIterator.hasNext());
		}

		public void test5Pages() throws Exception {

			//Get a connection manager based on the test properties
			ConnectionManagerTester cm = TestHelpers.getTestConnectionManager();
			cm.setLogCql(true);
			cm.buildCluster(true);

			CObjectShardList shardIdLists = new ShardListMock(Arrays.asList(1L,2L,3L,4L,5L));

			//Build our keyspace definition object
			CKeyspaceDefinition definition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "MultiInsertKeyspace.js");

			//Rebuild the keyspace and get the object mapper
			cm.buildKeyspace(definition, true);

			ObjectMapper om = cm.getObjectMapper(definition);
			om.setLogCql(true);

			// Set up test data
			// we will insert 200 objects
			int nDataItems = 200;

			List<Map<String, Object>> values2 = generateNObjects(nDataItems);

			List<Map<String, Object>> updatedValues2 = Lists.newArrayList();
			for (Map<String, Object> baseValue : values2) {
				updatedValues2.add(JsonUtil.rhombusMapFromJsonMap(baseValue, definition.getDefinitions().get("object2")));
			}

			Map<String, List<Map<String, Object>>> multiInsertMap = Maps.newHashMap();
			multiInsertMap.put("object2", updatedValues2);

			//Insert data
			om.insertBatchMixed(multiInsertMap);

			// generate a executorIterator
			SortedMap<String, Object> indexValues = Maps.newTreeMap();
			indexValues.put("account_id", UUID.fromString("00000003-0000-0030-0040-000000030000"));
			indexValues.put("user_id", UUID.fromString("00000003-0000-0030-0040-000000030000"));

			UUID stop = UUID.fromString(uuidList.get(nDataItems-1));
			CDefinition cDefinition = definition.getDefinitions().get("object2");
			BaseCQLStatementIterator unBoundedIterator = (BaseCQLStatementIterator) CObjectCQLGenerator.makeCQLforList(KEYSPACE_NAME, shardIdLists, cDefinition, indexValues, CObjectOrdering.DESCENDING, null, stop, 10l, true, false, false);
			Session session = cm.getRhombusSession(definition);
			CQLExecutor cqlExecutor = new CQLExecutor(session, true, definition.getConsistencyLevel());
			CQLExecutorIterator cqlExecutorIterator = new CQLExecutorIterator(cqlExecutor, unBoundedIterator);
			cqlExecutorIterator.setPageSize((nDataItems/5));


			for (int i=0 ; i< nDataItems; i++ ){
				assertTrue(cqlExecutorIterator.hasNext());
				assertNotNull(cqlExecutorIterator.next());
			}

			assertFalse(cqlExecutorIterator.hasNext());
		}
	}

	/**
	 * Create the test case
	 *
	 * @param testName name of the test case
	 */
	public CQLExecutorIteratorTest( String testName ) {
		super( testName );
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static junit.framework.Test suite() {
		return new TestSuite( CQLExecutorIteratorTest.class );
	}

	public void testIterator() throws Exception {
		Subject s = new Subject(0);
		s.testIterator();
	}

	public void testOneObject() throws Exception {
		Subject s = new Subject(0);
		s.testOneObject();
	}

	public void testPageSizePlus1() throws Exception {
		Subject s = new Subject(0);
		s.testPageSizePlus1();
	}

	public void test5Pages() throws Exception {
		Subject s = new Subject(0);
		s.test5Pages();
	}
}
