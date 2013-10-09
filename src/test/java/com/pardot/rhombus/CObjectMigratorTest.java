package com.pardot.rhombus;

import com.pardot.rhombus.cobject.*;
import com.pardot.rhombus.cobject.migrations.CObjectMigrationException;
import com.pardot.rhombus.cobject.migrations.CObjectMigrator;
import com.pardot.rhombus.cobject.shardingstrategy.ShardingStrategyNone;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * User: Rob Righter
 * Date: 10/8/13
 */
public class CObjectMigratorTest {


	@Test
	public void testIsMigratable() throws IOException {
		CDefinition OldDefinition = JsonUtil.objectFromJsonResource(CDefinition.class, this.getClass().getClassLoader(), "MigrationTestCDefinition.js");
		CDefinition NewDefinition = JsonUtil.objectFromJsonResource(CDefinition.class, this.getClass().getClassLoader(), "MigrationTestCDefinition.js");
		CIndex newIndex = new CIndex();
		newIndex.setKey("index_1:index_2");
		newIndex.setShardingStrategy(new ShardingStrategyNone());
		NewDefinition.getIndexes().put(newIndex.getName(), newIndex);
		CObjectMigrator subject = new CObjectMigrator(OldDefinition,NewDefinition);

		//since we only added an index it should be migratable
		assertTrue(subject.isMigratable());

		CField newField = new CField("newfield", CField.CDataType.VARCHAR);
		NewDefinition.getFields().put(newField.getName(),newField);

		//since now we added a new field it should NOT be migratable
		assertFalse(subject.isMigratable());

	}

	@Test
	public void testGetNewIndexes() throws IOException {
		CDefinition OldDefinition = JsonUtil.objectFromJsonResource(CDefinition.class, this.getClass().getClassLoader(), "MigrationTestCDefinition.js");
		CDefinition NewDefinition = JsonUtil.objectFromJsonResource(CDefinition.class, this.getClass().getClassLoader(), "MigrationTestCDefinition.js");

		CObjectMigrator subject = new CObjectMigrator(OldDefinition,NewDefinition);

		//No new indexes
		assertEquals(subject.getNewIndexes().size(), 0);

		//1 new index
		CIndex newIndex1 = new CIndex();
		newIndex1.setKey("index_1:index_2");
		newIndex1.setShardingStrategy(new ShardingStrategyNone());
		NewDefinition.getIndexes().put(newIndex1.getName(), newIndex1);
		assertEquals(subject.getNewIndexes().size(), 1);
		assertEquals(subject.getNewIndexes().get(0).getKey(), newIndex1.getKey());

		//2 new indexes
		CIndex newIndex2 = new CIndex();
		newIndex2.setKey("value");
		newIndex2.setShardingStrategy(new ShardingStrategyNone());
		NewDefinition.getIndexes().put(newIndex2.getName(), newIndex2);
		assertEquals(subject.getNewIndexes().size(), 2);
		assertEquals(subject.getNewIndexes().get(0).getKey(), newIndex2.getKey());
		assertEquals(subject.getNewIndexes().get(1).getKey(), newIndex1.getKey());
	}

	@Test
	public void testGetMigrationCQL() throws IOException, CObjectMigrationException {
		CDefinition OldDefinition = JsonUtil.objectFromJsonResource(CDefinition.class, this.getClass().getClassLoader(), "MigrationTestCDefinition.js");
		CDefinition NewDefinition = JsonUtil.objectFromJsonResource(CDefinition.class, this.getClass().getClassLoader(), "MigrationTestCDefinition.js");
		CIndex newIndex1 = new CIndex();
		newIndex1.setKey("index_1:index_2");
		newIndex1.setShardingStrategy(new ShardingStrategyNone());
		NewDefinition.getIndexes().put(newIndex1.getName(), newIndex1);
		CIndex newIndex2 = new CIndex();
		newIndex2.setKey("value");
		newIndex2.setShardingStrategy(new ShardingStrategyNone());
		NewDefinition.getIndexes().put(newIndex2.getName(), newIndex2);
		CObjectMigrator subject = new CObjectMigrator(OldDefinition,NewDefinition);
		assertEquals(subject.getNewIndexes().size(), 2);

		CQLStatementIterator result = subject.getMigrationCQL();
		assertEquals("CREATE TABLE \"simplef27e6d073810cfb7826cf964c67b383e\" (id timeuuid, shardid bigint, value varchar,index_1 varchar,index_2 varchar, PRIMARY KEY ((shardid, value),id) );", result.next().getQuery());
		assertEquals("CREATE TABLE \"simple02a6bb2fc3293d91f31c3f6ce892fedc\" (id timeuuid, shardid bigint, value varchar,index_1 varchar,index_2 varchar, PRIMARY KEY ((shardid, index_1, index_2),id) );", result.next().getQuery());
		assertEquals(false, result.hasNext());
	}
}
