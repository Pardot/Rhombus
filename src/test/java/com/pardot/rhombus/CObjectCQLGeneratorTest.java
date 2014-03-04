package com.pardot.rhombus;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.cobject.*;
import com.pardot.rhombus.cobject.statement.CQLStatement;
import com.pardot.rhombus.cobject.statement.CQLStatementIterator;
import com.pardot.rhombus.helpers.TestHelpers;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 4/9/13
 */
public class CObjectCQLGeneratorTest  extends TestCase {

	static final String KEYSPACE_NAME = "testspace";
	static final String TABLE_NAME = "testtype";

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

		public Subject(Integer consistencyHorizon){
			super(KEYSPACE_NAME, consistencyHorizon);
		}

		public void testMakeStaticTableCreate() throws CObjectParseException, IOException {
			String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
			CDefinition def = CDefinition.fromJsonString(json);
			Subject subject = new Subject(1000);
			CQLStatement cql = subject.makeStaticTableCreate(def);
			CQLStatement expected = CQLStatement.make(
					"CREATE TABLE \"testspace\".\"testtype\" (id timeuuid PRIMARY KEY, filtered int,data1 varchar,data2 varchar,data3 varchar,instance bigint,type int,foreignid bigint);",
					TABLE_NAME
			);
			assertEquals(expected, cql);
		}

		public void testMakeWideTableCreate() throws CObjectParseException, IOException {
			String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
			CDefinition def = CDefinition.fromJsonString(json);
			Subject subject = new Subject(1000);
			CQLStatement cql1 = subject.makeWideTableCreate(def, def.getIndexes().get("foreignid"));
			CQLStatement expected1 = CQLStatement.make(
					"CREATE TABLE \"testspace\".\"testtype7f9bb4e56d3cae5b11c553547cfe5897\" (id timeuuid, shardid bigint, filtered int,data1 varchar,data2 varchar,data3 varchar,instance bigint,type int,foreignid bigint, PRIMARY KEY ((shardid, foreignid),id) );",
					TABLE_NAME
			);
			assertEquals(expected1, cql1);

			CQLStatement cql2 = subject.makeWideTableCreate(def, def.getIndexes().get("instance:type"));
			CQLStatement expected2 = CQLStatement.make(
					"CREATE TABLE \"testspace\".\"testtype6671808f3f51bcc53ddc76d2419c9060\" (id timeuuid, shardid bigint, filtered int,data1 varchar,data2 varchar,data3 varchar,instance bigint,type int,foreignid bigint, PRIMARY KEY ((shardid, instance, type),id) );",
					TABLE_NAME
			);
			assertEquals(expected2, cql2);

			CQLStatement cql3 = subject.makeWideTableCreate(def, def.getIndexes().get("foreignid:instance:type"));
			CQLStatement expected3 =  CQLStatement.make(
					"CREATE TABLE \"testspace\".\"testtypef9bf3332bb4ec879849ec43c67776131\" (id timeuuid, shardid bigint, filtered int,data1 varchar,data2 varchar,data3 varchar,instance bigint,type int,foreignid bigint, PRIMARY KEY ((shardid, foreignid, instance, type),id) );",
					TABLE_NAME
			);
			assertEquals(expected3, cql3);
		}

		public void testMakeCQLforInsert() throws CQLGenerationException, CObjectParseException, IOException {
			String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
			CDefinition def = CDefinition.fromJsonString(json);
			Map<String, Object> data = TestHelpers.getTestObject(0);
			UUID uuid = UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43");
			CQLStatementIterator result = Subject.makeCQLforInsert(KEYSPACE_NAME, def,data,uuid,Long.valueOf(1),null);
			List<CQLStatement> actual = toList(result);

			CQLStatement expected;
			assertEquals("Should generate CQL statements for the static table plus all indexes including the filtered index", 6, actual.size());
			//static table
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtype\" (id, filtered, data1, data2, data3, instance, type, foreignid) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList(uuid, 1, "This is data one", "This is data two", "This is data three", 222222, 5, 777).toArray()
			);
			assertEquals(expected, actual.get(0));
			Object[] expectedValues = Arrays.asList(uuid, Long.valueOf(160),1, "This is data one", "This is data two", "This is data three", 222222, 5, 777).toArray();
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtype6671808f3f51bcc53ddc76d2419c9060\" (id, shardid, filtered, data1, data2, data3, instance, type, foreignid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
					TABLE_NAME,
					expectedValues
			);
			assertEquals(expected, actual.get(1));

			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"__shardindex\" (tablename, indexvalues, shardid, targetrowkey) VALUES (?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList("testtype6671808f3f51bcc53ddc76d2419c9060","222222:5",Long.valueOf(160),"160:222222:5").toArray()
			);
			assertEquals(expected, actual.get(2));


			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtypef9bf3332bb4ec879849ec43c67776131\" (id, shardid, filtered, data1, data2, data3, instance, type, foreignid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
					TABLE_NAME,
					expectedValues
			);
			assertEquals(expected,actual.get(3));


			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"__shardindex\" (tablename, indexvalues, shardid, targetrowkey) VALUES (?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList("testtypef9bf3332bb4ec879849ec43c67776131","777:222222:5",Long.valueOf(160),"160:777:222222:5").toArray()
			);
			assertEquals(expected, actual.get(4));

			expectedValues = Arrays.asList(uuid, Long.valueOf(1),1, "This is data one", "This is data two", "This is data three", 222222, 5, 777).toArray();
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtype7f9bb4e56d3cae5b11c553547cfe5897\" (id, shardid, filtered, data1, data2, data3, instance, type, foreignid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
					TABLE_NAME,
					expectedValues
			);
			assertEquals(expected,actual.get(5));
			//foreign has shard strategy None so we dont expect an insert into the shard index table

			//test with ttl
			result = Subject.makeCQLforInsert(KEYSPACE_NAME, def,data,uuid,Long.valueOf(1),Integer.valueOf(20));
			actual = toList(result);
			expectedValues = Arrays.asList(uuid, 1, "This is data one", "This is data two", "This is data three", 222222, 5, 777).toArray();
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtype\" (id, filtered, data1, data2, data3, instance, type, foreignid) VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL 20;",
					TABLE_NAME,
					expectedValues
			);
			assertEquals(expected, actual.get(0));

			//test with inserting less than all of the fields
			data = TestHelpers.getTestObject(3);
			uuid = UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43");
			result = Subject.makeCQLforInsert(KEYSPACE_NAME, def,data,uuid,Long.valueOf(1),null);
			actual = toList(result);

			assertEquals("Should generate CQL statements for the static table plus all indexes including the filtered index", 6, actual.size());
			//static table
			expectedValues = Arrays.asList(uuid, 1, "This is data one", 222222, 5, 777).toArray();
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtype\" (id, filtered, data1, instance, type, foreignid) VALUES (?, ?, ?, ?, ?, ?);",
					TABLE_NAME,
					expectedValues
			);
			assertEquals(expected, actual.get(0));

			expectedValues = Arrays.asList(uuid, Long.valueOf(160), 1, "This is data one", 222222, 5, 777).toArray();
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtype6671808f3f51bcc53ddc76d2419c9060\" (id, shardid, filtered, data1, instance, type, foreignid) VALUES (?, ?, ?, ?, ?, ?, ?);",
					TABLE_NAME,
					expectedValues
			);
			assertEquals(expected, actual.get(1));

			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"__shardindex\" (tablename, indexvalues, shardid, targetrowkey) VALUES (?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList("testtype6671808f3f51bcc53ddc76d2419c9060","222222:5",Long.valueOf(160),"160:222222:5").toArray()
			);
			assertEquals(expected, actual.get(2));

			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtypef9bf3332bb4ec879849ec43c67776131\" (id, shardid, filtered, data1, instance, type, foreignid) VALUES (?, ?, ?, ?, ?, ?, ?);",
					TABLE_NAME,
					expectedValues
			);
			assertEquals(expected,actual.get(3));

			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"__shardindex\" (tablename, indexvalues, shardid, targetrowkey) VALUES (?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList("testtypef9bf3332bb4ec879849ec43c67776131","777:222222:5",Long.valueOf(160),"160:777:222222:5").toArray()
			);
			assertEquals(expected, actual.get(4));

			expectedValues = Arrays.asList(uuid, Long.valueOf(1), 1, "This is data one", 222222, 5, 777).toArray();
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtype7f9bb4e56d3cae5b11c553547cfe5897\" (id, shardid, filtered, data1, instance, type, foreignid) VALUES (?, ?, ?, ?, ?, ?, ?);",
					TABLE_NAME,
					expectedValues
			);
			assertEquals(expected,actual.get(5));
			//foreign has shard strategy None so we dont expect an insert into the shard index table


			//test that an exception is thrown if you try to insert missing an index primary key with allowNullPrimaryKeyInserts = false
			assertTrue(!def.isAllowNullPrimaryKeyInserts());
			data = Maps.newHashMap();
			data.put("data1","this is a test");
			uuid = UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43");
			try{
				Subject.makeCQLforInsert(KEYSPACE_NAME, def,data,uuid,Long.valueOf(1),null);
				assertTrue("Should never get here", false);
			}
			catch (CQLGenerationException e){
				assertTrue("Should throw cqlGenerationException if missing index fields and allowNullPrimaryKeyInserts = false", true);
			}

			//test that an insert is allowed and that the associated indexes are ignored if you try to insert missing an index primary key with allowNullPrimaryKeyInserts = true
			def.setAllowNullPrimaryKeyInserts(true);
			assertTrue(def.isAllowNullPrimaryKeyInserts());
			data.put("data1","this is a test");
			data.put("foreignid", "777");
			uuid = UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43");
			result = Subject.makeCQLforInsert(KEYSPACE_NAME, def,data,uuid,Long.valueOf(1),null);
			actual = toList(result);
			assertEquals("Number of CQL statements should be correct",2,actual.size());
			//static table
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtype\" (id, data1, foreignid) VALUES (?, ?, ?);",
					TABLE_NAME,
					Arrays.asList(uuid, "this is a test", "777").toArray()
			);
			assertEquals(expected, actual.get(0));

			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtype7f9bb4e56d3cae5b11c553547cfe5897\" (id, shardid, data1, foreignid) VALUES (?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList(uuid, Long.valueOf(1), "this is a test", "777").toArray()
			);
			assertEquals(expected,actual.get(1));
			//foreign has shard strategy None so we dont expect an insert into the shard index table
		}

		public void testMakeCQLforCreate() throws CObjectParseException, IOException {
			String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
			CDefinition def = CDefinition.fromJsonString(json);
			Subject subject = new Subject(1000);
			CQLStatementIterator actual = subject.makeCQLforCreate(def);
			assertEquals("Should generate CQL statements for the static table plus all indexes", 4, actual.size());
		}

		public void testMakeCQLforGet() throws CObjectParseException,CObjectParseException, CQLGenerationException, IOException {
			String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
			CDefinition def = CDefinition.fromJsonString(json);

			//Static Table Get
			UUID uuid = UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43");
			CQLStatementIterator actual = Subject.makeCQLforGet(KEYSPACE_NAME, def, uuid);
			assertEquals("Static gets should return bounded query iterator", true,actual.isBounded());
			assertEquals("Static gets should return an iterator with 1 statement", 1,actual.size());
			Object[] expectedValues = {uuid};
			CQLStatement expected = CQLStatement.make("SELECT * FROM \"testspace\".\"testtype\" WHERE id = ?;", TABLE_NAME, expectedValues);
			assertEquals("Should generate proper CQL for static table get by ID", expected, toList(actual).get(0));

			CObjectShardList shardIdLists = new ShardListMock(Arrays.asList(1L,2L,3L,4L,5L));
			UUID start = UUID.fromString("a8a2abe0-a251-11e2-bcbb-adf1a79a327f");
			UUID stop = UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43");
			//Wide table using shardIdList and therefore bounded
			TreeMap<String,Object> indexkeys = Maps.newTreeMap();
			indexkeys.put("foreignid","777");
			indexkeys.put("type", "5");
			indexkeys.put("instance", "222222");
			actual = CObjectCQLGenerator.makeCQLforList(KEYSPACE_NAME, shardIdLists, def, indexkeys, CObjectOrdering.DESCENDING, null, UUIDs.startOf(DateTime.now().getMillis()), 10l, false, false, false);
			expected = CQLStatement.make(
					"SELECT * FROM \"testspace\".\"testtypef9bf3332bb4ec879849ec43c67776131\" WHERE shardid = ? AND foreignid = ? AND instance = ? AND type = ? AND id < ? ORDER BY id DESC LIMIT 10 ALLOW FILTERING;",
					TABLE_NAME,
					arrayFromValues(Long.valueOf(1),"777","222222","5", stop)
			);
			CQLStatement result = actual.next();
			assertEquals(expected.getQuery(), result.getQuery());
			assertEquals(expected.getValues()[0], result.getValues()[0]);
			assertEquals(expected.getValues()[1], result.getValues()[1]);
			assertEquals(expected.getValues()[2], result.getValues()[2]);
			assertEquals(expected.getValues()[3], result.getValues()[3]);

			//expected = "SELECT * FROM \"testtypef9bf3332bb4ec879849ec43c67776131\" WHERE shardid = 2 AND foreignid = 777 AND instance = 222222 AND type = 5 AND id <";
			expected = CQLStatement.make(
					"SELECT * FROM \"testspace\".\"testtypef9bf3332bb4ec879849ec43c67776131\" WHERE shardid = ? AND foreignid = ? AND instance = ? AND type = ? AND id < ? ORDER BY id DESC LIMIT 10 ALLOW FILTERING;",
					TABLE_NAME,
					Arrays.asList(Long.valueOf(2),"777","222222","5", stop).toArray()
			);
			result = actual.next();
			assertEquals(expected.getQuery(), result.getQuery());
			assertEquals(expected.getValues()[0], result.getValues()[0]);
			assertEquals(expected.getValues()[1], result.getValues()[1]);
			assertEquals(expected.getValues()[2], result.getValues()[2]);
			assertEquals(expected.getValues()[3], result.getValues()[3]);
			assertEquals("Should be bounded query list", true, actual.isBounded());


			//Wide table exclusive slice bounded query should not use shard list
			indexkeys = Maps.newTreeMap();
			indexkeys.put("foreignid","777");
			indexkeys.put("type", "5");
			indexkeys.put("instance", "222222");
			actual = CObjectCQLGenerator.makeCQLforList(KEYSPACE_NAME, shardIdLists, def, indexkeys, CObjectOrdering.DESCENDING, start, stop, 10l, false, false, false);
			expected = CQLStatement.make(
					"SELECT * FROM \"testspace\".\"testtypef9bf3332bb4ec879849ec43c67776131\" WHERE shardid = ? AND foreignid = ? AND instance = ? AND type = ? AND id > ? AND id < ? ORDER BY id DESC LIMIT 10 ALLOW FILTERING;",
					TABLE_NAME,
					arrayFromValues(Long.valueOf(160),"777","222222","5", start, stop)
			);
			expected.setCacheable(true);
			//"Should generate proper CQL for wide table get by index values"
			CQLStatement actualStatement = actual.next();
			assertEquals(expected, actualStatement);
			assertTrue("Should be bounded query iterator", actual.isBounded());
			assertTrue("Should be none remaining in the iterator", !actual.hasNext());


			//wide table inclusive slice ascending bounded
			start = UUID.fromString("b4c10d80-15f0-11e0-8080-808080808080"); // 1/1/2011 long startd = 1293918439000L;
			stop = UUID.fromString("2d87f48f-34c2-11e1-7f7f-7f7f7f7f7f7f"); //1/1/2012 long endd = 1325454439000L;
			actual = CObjectCQLGenerator.makeCQLforList(KEYSPACE_NAME, shardIdLists, def, indexkeys, CObjectOrdering.ASCENDING, start, stop, 10l, true, false, false);
			assertEquals("Should be proper size for range", 13, actual.size()); //All of 2011 plus the first month of 2012
			expected = CQLStatement.make(
					"SELECT * FROM \"testspace\".\"testtypef9bf3332bb4ec879849ec43c67776131\" WHERE shardid = ? AND foreignid = ? AND instance = ? AND type = ? AND id >= ? AND id <= ? ORDER BY id ASC LIMIT 10 ALLOW FILTERING;",
					TABLE_NAME,
					arrayFromValues(Long.valueOf(133),"777","222222","5",start,stop)
			);
			expected.setCacheable(true);
			//Should generate proper CQL for wide table get by index values"
			actualStatement = actual.next();
			assertEquals(expected, actualStatement);

			expected = CQLStatement.make(
					"SELECT * FROM \"testspace\".\"testtypef9bf3332bb4ec879849ec43c67776131\" WHERE shardid = ? AND foreignid = ? AND instance = ? AND type = ? AND id >= ? AND id <= ? ORDER BY id ASC LIMIT 10 ALLOW FILTERING;",
					TABLE_NAME,
					Arrays.asList(Long.valueOf(134),"777","222222","5",start,stop).toArray()
			);
			expected.setCacheable(true);
			//Should generate proper CQL for wide table get by index values
			assertEquals(expected,actual.next());

			expected = CQLStatement.make(
					"SELECT * FROM \"testspace\".\"testtypef9bf3332bb4ec879849ec43c67776131\" WHERE shardid = ? AND foreignid = ? AND instance = ? AND type = ? AND id >= ? AND id <= ? ORDER BY id ASC LIMIT 5 ALLOW FILTERING;",
					TABLE_NAME,
					Arrays.asList(Long.valueOf(135),"777","222222","5",start,stop).toArray()
			);
			expected.setCacheable(true);
			assertTrue("Should have next when hinted less than the limit",actual.hasNext(5));
			//"Should generate proper Limit adjustment when given the amount hint"
			assertEquals(expected,actual.next());
			assertTrue("Should have no next when hinted more than or equal to the limit",!actual.hasNext(10));

			//wide table inclusive slice descending bounded
			start = UUID.fromString("b4c10d80-15f0-11e0-8080-808080808080"); // 1/1/2011 long startd = 1293918439000L;
			stop = UUID.fromString("2d87f48f-34c2-11e1-7f7f-7f7f7f7f7f7f"); //1/1/2012 long endd = 1325454439000L;
			actual = Subject.makeCQLforList(KEYSPACE_NAME, shardIdLists, def, indexkeys, CObjectOrdering.DESCENDING, start, stop, 10l, true, false, false);
			assertEquals("Descending: Should be proper size for range", 13, actual.size()); //All of 2011 plus the first month of 2012
			expected = CQLStatement.make(
					"SELECT * FROM \"testspace\".\"testtypef9bf3332bb4ec879849ec43c67776131\" WHERE shardid = ? AND foreignid = ? AND instance = ? AND type = ? AND id >= ? AND id <= ? ORDER BY id DESC LIMIT 10 ALLOW FILTERING;",
					TABLE_NAME,
					arrayFromValues(Long.valueOf(145),"777","222222","5",start,stop)
			);
			expected.setCacheable(true);
			//"Descending: Should generate proper CQL for wide table get by index values"
			assertEquals(expected,actual.next());
			expected = CQLStatement.make(
					"SELECT * FROM \"testspace\".\"testtypef9bf3332bb4ec879849ec43c67776131\" WHERE shardid = ? AND foreignid = ? AND instance = ? AND type = ? AND id >= ? AND id <= ? ORDER BY id DESC LIMIT 10 ALLOW FILTERING;",
					TABLE_NAME,
					Arrays.asList(Long.valueOf(144),"777","222222","5",start,stop).toArray()
			);
			expected.setCacheable(true);
			assertEquals("Descending: Should generate proper CQL for wide table get by index values",expected,actual.next());
			expected = CQLStatement.make(
					"SELECT * FROM \"testspace\".\"testtypef9bf3332bb4ec879849ec43c67776131\" WHERE shardid = ? AND foreignid = ? AND instance = ? AND type = ? AND id >= ? AND id <= ? ORDER BY id DESC LIMIT 5 ALLOW FILTERING;",
					TABLE_NAME,
					Arrays.asList(Long.valueOf(143),"777","222222","5",start,stop).toArray()
			);
			expected.setCacheable(true);
			assertTrue("Descending: Should have next when hinted less than the limit",actual.hasNext(5));
			assertEquals("Descending: Should generate proper Limit adjustment when given the amount hint",expected,actual.next());
			assertTrue("Should have no next when hinted more than or equal to the limit",!actual.hasNext(10));

		}

		public void testMakeCQLforDelete() throws CObjectParseException,CObjectParseException, CQLGenerationException, IOException {
			String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
			CDefinition def = CDefinition.fromJsonString(json);
			Map<String, Object> data = TestHelpers.getTestObject(0);
			UUID uuid = UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43");
			CQLStatementIterator result = Subject.makeCQLforDelete(KEYSPACE_NAME, def,uuid,data,Long.valueOf(111));

			CQLStatement expected;

			expected = CQLStatement.make("DELETE FROM \"testspace\".\"testtype\" WHERE id = ?;", TABLE_NAME, Arrays.asList(uuid).toArray());
			assertEquals(expected,result.next());

			expected = CQLStatement.make(
					"DELETE FROM \"testspace\".\"testtype6671808f3f51bcc53ddc76d2419c9060\" WHERE id = ? AND shardid = ? AND instance = ? AND type = ?;",
					TABLE_NAME,
					Arrays.asList(uuid,Long.valueOf(160), 222222, 5).toArray());
			assertEquals(expected,result.next());
			expected = CQLStatement.make(
					"DELETE FROM \"testspace\".\"testtypef9bf3332bb4ec879849ec43c67776131\" WHERE id = ? AND shardid = ? AND foreignid = ? AND instance = ? AND type = ?;",
					TABLE_NAME,
					Arrays.asList(uuid,Long.valueOf(160), 777, 222222, 5).toArray());
			assertEquals(expected,result.next());
			expected = CQLStatement.make(
					"DELETE FROM \"testspace\".\"testtype7f9bb4e56d3cae5b11c553547cfe5897\" WHERE id = ? AND shardid = ? AND foreignid = ?;",
					TABLE_NAME,
					Arrays.asList(uuid,Long.valueOf(1), 777).toArray());
			assertEquals(expected,result.next());
			assertTrue(!result.hasNext());

		}

		public void testMakeIndexTableName(){

			//First test right at the character limit
			CDefinition def = new CDefinition();
			def.setName("a123456789012345");
			CIndex index = new CIndex();
			index.setKey("this:is:some:key");
			assertEquals("a123456789012345cfd257aab86bf8ae49f975e12d9636cd", makeIndexTableName(def,index));
			assertEquals(48, makeIndexTableName(def,index).length());

			//now test after the character limit
			def.setName("a1234567890123423232323");
			assertEquals("a123456789012342cf14e7ebcd6250a6d856375dc9aae588", makeIndexTableName(def,index));
			assertEquals(48, makeIndexTableName(def,index).length());

			//now test before the character limit
			def.setName("short");
			assertEquals("shortd7e4bfec2b32954decd1fdbc11052e9d", makeIndexTableName(def,index));
			assertTrue(48 > makeIndexTableName(def,index).length());
		}

		public void testMakeCQLforUpdate() throws CObjectParseException,CObjectParseException, CQLGenerationException, IOException{
			String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
			CDefinition def = CDefinition.fromJsonString(json);
			Map<String, Object> data = TestHelpers.getTestObject(0);
			UUID uuid = UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43");
			Map<String,Object> newdata = Maps.newHashMap();
			newdata.put("type", Integer.valueOf(9));
			CQLStatementIterator result = Subject.makeCQLforUpdate(KEYSPACE_NAME, def,uuid,data,newdata);
			CQLStatement expected = CQLStatement.make(
					"DELETE FROM \"testspace\".\"testtype6671808f3f51bcc53ddc76d2419c9060\" WHERE id = ? AND shardid = ? AND instance = ? AND type = ?;",
					TABLE_NAME,
					Arrays.asList(UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43"), Long.valueOf(160), Integer.valueOf(222222), Integer.valueOf(5)).toArray()
			);
			assertEquals(expected, result.next());
			expected = CQLStatement.make(
					"DELETE FROM \"testspace\".\"testtypef9bf3332bb4ec879849ec43c67776131\" WHERE id = ? AND shardid = ? AND foreignid = ? AND instance = ? AND type = ?;",
					TABLE_NAME,
					Arrays.asList(UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43"), Long.valueOf(160), Integer.valueOf(777), Integer.valueOf(222222), Integer.valueOf(5)).toArray()
			);
			assertEquals(expected, result.next());
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtype6671808f3f51bcc53ddc76d2419c9060\" (id, shardid, filtered, data1, data2, data3, instance, type, foreignid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList(
					UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43"),
					Long.valueOf(160),
					Integer.valueOf(1),
					"This is data one",
					"This is data two",
					"This is data three",
					Integer.valueOf(222222),
					Integer.valueOf(9),
					Integer.valueOf(777)).toArray()
			);
			assertEquals(expected, result.next());
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"__shardindex\" (tablename, indexvalues, shardid, targetrowkey) VALUES (?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList(
						"testtype6671808f3f51bcc53ddc76d2419c9060",
						"222222:9",
						Long.valueOf(160),
						"160:222222:9").toArray()
			);
			assertEquals(expected, result.next());
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtypef9bf3332bb4ec879849ec43c67776131\" (id, shardid, filtered, data1, data2, data3, instance, type, foreignid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList(
						UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43"),
						Long.valueOf(160),
						Integer.valueOf(1),
						"This is data one",
						"This is data two",
						"This is data three",
						Integer.valueOf(222222),
						Integer.valueOf(9),
						Integer.valueOf(777)).toArray()
			);
			assertEquals(expected, result.next());
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"__shardindex\" (tablename, indexvalues, shardid, targetrowkey) VALUES (?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList(
						"testtypef9bf3332bb4ec879849ec43c67776131",
						"777:222222:9",
						Long.valueOf(160),
						"160:777:222222:9").toArray()
			);
			assertEquals(expected, result.next());
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtype7f9bb4e56d3cae5b11c553547cfe5897\" (id, shardid, instance, type, foreignid) VALUES (?, ?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList(
						UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43"),
						Long.valueOf(1),
						222222,
						9,
						777).toArray()
			);
			assertEquals(expected, result.next());
			//verify that this last insert was on the uneffected index (which is why it does not have a matching __shardindex insert
			assertEquals("testtype7f9bb4e56d3cae5b11c553547cfe5897",makeTableName(def,def.getIndexes().get("foreignid")));
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtype\" (id, type) VALUES (?, ?);",
					TABLE_NAME,
					Arrays.asList(
						UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43"),
						Integer.valueOf(9)).toArray()
			);
			assertEquals(expected, result.next());
			CQLStatement next = result.next();
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"__index_updates\" (id, statictablename, instanceid, indexvalues) values (?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList(
							(UUID)next.getValues()[0],
							"testtype",
							UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43"),
							"{\"foreignid\":777,\"instance\":222222,\"type\":9}").toArray()
			);
			assertEquals(expected, next);
			//should be no results left
			assertTrue(!result.hasNext());




			//Now try the same update, but this time we dont change anything and send the same values. In this case
			//It should not generate any deletes
			newdata.put("type", Integer.valueOf(5));
			 result = Subject.makeCQLforUpdate(KEYSPACE_NAME, def,uuid,data,newdata);
			//no deletes
			//Go right to the inserts
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtype6671808f3f51bcc53ddc76d2419c9060\" (id, shardid, filtered, data1, data2, data3, instance, type, foreignid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList(
							UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43"),
							Long.valueOf(160),
							Integer.valueOf(1),
							"This is data one",
							"This is data two",
							"This is data three",
							Integer.valueOf(222222),
							Integer.valueOf(5),
							Integer.valueOf(777)).toArray()
			);
			assertEquals(expected, result.next());
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"__shardindex\" (tablename, indexvalues, shardid, targetrowkey) VALUES (?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList(
							"testtype6671808f3f51bcc53ddc76d2419c9060",
							"222222:5",
							Long.valueOf(160),
							"160:222222:5").toArray()
			);
			assertEquals(expected, result.next());
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtypef9bf3332bb4ec879849ec43c67776131\" (id, shardid, filtered, data1, data2, data3, instance, type, foreignid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList(
							UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43"),
							Long.valueOf(160),
							Integer.valueOf(1),
							"This is data one",
							"This is data two",
							"This is data three",
							Integer.valueOf(222222),
							Integer.valueOf(5),
							Integer.valueOf(777)).toArray()
			);
			assertEquals(expected, result.next());
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"__shardindex\" (tablename, indexvalues, shardid, targetrowkey) VALUES (?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList(
							"testtypef9bf3332bb4ec879849ec43c67776131",
							"777:222222:5",
							Long.valueOf(160),
							"160:777:222222:5").toArray()
			);
			assertEquals(expected, result.next());
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtype7f9bb4e56d3cae5b11c553547cfe5897\" (id, shardid, instance, type, foreignid) VALUES (?, ?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList(
							UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43"),
							Long.valueOf(1),
							222222,
							5,
							777).toArray()
			);
			assertEquals(expected, result.next());
			//verify that this last insert was on the uneffected index (which is why it does not have a matching __shardindex insert
			assertEquals("testtype7f9bb4e56d3cae5b11c553547cfe5897",makeTableName(def,def.getIndexes().get("foreignid")));
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"testtype\" (id, type) VALUES (?, ?);",
					TABLE_NAME,
					Arrays.asList(
							UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43"),
							Integer.valueOf(5)).toArray()
			);
			assertEquals(expected, result.next());
			next = result.next();
			expected = CQLStatement.make(
					"INSERT INTO \"testspace\".\"__index_updates\" (id, statictablename, instanceid, indexvalues) values (?, ?, ?, ?);",
					TABLE_NAME,
					Arrays.asList(
							(UUID)next.getValues()[0],
							"testtype",
							UUID.fromString("ada375b0-a2d9-11e2-99a3-3f36d3955e43"),
							"{\"foreignid\":777,\"instance\":222222,\"type\":5}").toArray()
			);
			assertEquals(expected, next);
			//should be no results left
			assertTrue(!result.hasNext());


		}

	}

	public static List<CQLStatement> toList(CQLStatementIterator i){
		List<CQLStatement> ret = Lists.newArrayList();
		if(!i.isBounded()){
			return ret;
		}
		while(i.hasNext()){
			ret.add(i.next());
		}
		return ret;
	}

	/**
	 * Create the test case
	 *
	 * @param testName name of the test case
	 */
	public CObjectCQLGeneratorTest( String testName ) {
		super( testName );
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite() {
		return new TestSuite( CObjectCQLGeneratorTest.class );
	}

	public void testMakeStaticTableCreate() throws CObjectParseException, IOException {
		Subject s = new Subject(0);
		s.testMakeStaticTableCreate();
	}

	public void testMakeWideTableCreate() throws CObjectParseException, IOException {
		Subject s = new Subject(0);
		s.testMakeWideTableCreate();
	}

	public void testMakeCQLforCreate() throws CObjectParseException, IOException {
		Subject s = new Subject(0);
		s.testMakeCQLforCreate();
	}

	public void testMakeCQLforInsert() throws CQLGenerationException, CObjectParseException, IOException {
		Subject s = new Subject(0);
		s.testMakeCQLforInsert();
	}

	public void testMakeCQLforGet() throws CQLGenerationException, CObjectParseException, IOException {
		Subject s = new Subject(0);
		s.testMakeCQLforGet();
	}

	public void testMakeCQLforDelete() throws CQLGenerationException, CObjectParseException, IOException {
		Subject s = new Subject(0);
		s.testMakeCQLforDelete();
	}

	public void testMakeIndexTableName() throws CQLGenerationException, CObjectParseException, IOException {
		Subject s = new Subject(0);
		s.testMakeIndexTableName();
	}

	public void testMakeCQLforUpdate() throws CQLGenerationException, CObjectParseException, IOException {
		Subject s = new Subject(0);
		s.testMakeCQLforUpdate();
	}

	private Object[] arrayFromValues(Object... args) {
		Object[] ret = new Object[args.length];
		int index = 0;
		for(Object obj : args) {
			ret[index] = obj;
			index++;
		}
		return ret;
	}
}
