package com.pardot.rhombus;

import com.pardot.rhombus.cobject.*;
import com.pardot.rhombus.cobject.migrations.CKeyspaceDefinitionMigrator;
import com.pardot.rhombus.cobject.migrations.CObjectMigrationException;
import com.pardot.rhombus.cobject.shardingstrategy.ShardingStrategyNone;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;

import javax.validation.constraints.AssertTrue;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

/**
 * User: Rob Righter
 * Date: 10/8/13
 */
public class CKeyspaceDefinitionMigratorTest {

	@Test
	public void testIsMigratable() throws IOException {
		CKeyspaceDefinition OldKeyspaceDefinition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		CKeyspaceDefinition NewKeyspaceDefinition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		CDefinition NewObjectDefinition = JsonUtil.objectFromJsonResource(CDefinition.class, this.getClass().getClassLoader(), "MigrationTestCDefinition.js");
		NewKeyspaceDefinition.getDefinitions().put(NewObjectDefinition.getName(),NewObjectDefinition);

		CKeyspaceDefinitionMigrator subject = new CKeyspaceDefinitionMigrator(OldKeyspaceDefinition, NewKeyspaceDefinition, "rhombus_data");
		assertTrue(subject.isMigratable());

		//now try adding an field (which is now supported)
		NewKeyspaceDefinition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		CField newField = new CField("newfield", CField.CDataType.VARCHAR);
		NewKeyspaceDefinition.getDefinitions().get("testtype").getFields().put(newField.getName(),newField);
		subject = new CKeyspaceDefinitionMigrator(OldKeyspaceDefinition, NewKeyspaceDefinition, "rhombus_data");
		assertTrue(subject.isMigratable());

		//now try changing the type of a field (which is not supported)
		NewKeyspaceDefinition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		CField changedField = new CField("filtered", CField.CDataType.VARCHAR);
		NewKeyspaceDefinition.getDefinitions().get("testtype").getFields().put(changedField.getName(),changedField);
		subject = new CKeyspaceDefinitionMigrator(OldKeyspaceDefinition, NewKeyspaceDefinition, "rhombus_data");
		assertFalse(subject.isMigratable());

		//now change it back and it should work
		changedField = new CField("filtered", CField.CDataType.INT);
		NewKeyspaceDefinition.getDefinitions().get("testtype").getFields().put(changedField.getName(),changedField);
		subject = new CKeyspaceDefinitionMigrator(OldKeyspaceDefinition, NewKeyspaceDefinition, "rhombus_data");
		assertTrue(subject.isMigratable());

		//now add an ID field which is not supported (because you can never change the type of the id)
		//now try adding an field (which is now supported)
		NewKeyspaceDefinition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		newField = new CField("id", CField.CDataType.VARCHAR);
		NewKeyspaceDefinition.getDefinitions().get("testtype").getFields().put(newField.getName(),newField);
		subject = new CKeyspaceDefinitionMigrator(OldKeyspaceDefinition, NewKeyspaceDefinition, "rhombus_data");
		assertFalse(subject.isMigratable());

	}

	@Test
	public void testGetMigrationCQL() throws IOException, CObjectMigrationException {
		CKeyspaceDefinition OldKeyspaceDefinition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		CKeyspaceDefinition NewKeyspaceDefinition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		//add a new index to existing object
		CIndex newIndex1 = new CIndex();
		newIndex1.setKey("data1:data2");
		newIndex1.setShardingStrategy(new ShardingStrategyNone());
		NewKeyspaceDefinition.getDefinitions().get("testtype").getIndexes().put(newIndex1.getName(), newIndex1);
		//add a new field to existing object
		CField newField = new CField("newfield", CField.CDataType.VARCHAR);
		NewKeyspaceDefinition.getDefinitions().get("testtype").getFields().put(newField.getName(), newField);

		//add new object
		CDefinition NewObjectDefinition = JsonUtil.objectFromJsonResource(CDefinition.class, this.getClass().getClassLoader(), "MigrationTestCDefinition.js");
		NewKeyspaceDefinition.getDefinitions().put(NewObjectDefinition.getName(),NewObjectDefinition);

		//Construct the object. It should be migratable
		CKeyspaceDefinitionMigrator subject = new CKeyspaceDefinitionMigrator(OldKeyspaceDefinition, NewKeyspaceDefinition, "rhombus_data");
		assertTrue(subject.isMigratable());

		//Now verify that the correct CQL is generated for the migration
		CObjectCQLGenerator cqlGenerator = new CObjectCQLGenerator(OldKeyspaceDefinition.getName(), 1000);
		CQLStatementIterator result = subject.getMigrationCQL(cqlGenerator);
		assertEquals("ALTER TABLE \"functional\".\"testtype\" add newfield varchar", result.next().getQuery());
		assertEquals("ALTER TABLE \"functional\".\"testtype6671808f3f51bcc53ddc76d2419c9060\" add newfield varchar", result.next().getQuery());
		assertEquals("ALTER TABLE \"functional\".\"testtypef9bf3332bb4ec879849ec43c67776131\" add newfield varchar", result.next().getQuery());
		assertEquals("ALTER TABLE \"functional\".\"testtype7f9bb4e56d3cae5b11c553547cfe5897\" add newfield varchar", result.next().getQuery());

		assertEquals("CREATE TABLE \"functional\".\"testtypeb4e47a87138afd20159a6522134a3bc2\" (id timeuuid, shardid bigint, newfield varchar,filtered int,data1 varchar,data2 varchar,data3 varchar,instance bigint,type int,foreignid bigint, PRIMARY KEY ((shardid, data1, data2),id) );",result.next().getQuery());
		assertEquals("CREATE TABLE \"functional\".\"simple\" (id timeuuid PRIMARY KEY, value varchar,index_1 varchar,index_2 varchar);", result.next().getQuery());
		assertEquals("CREATE TABLE \"functional\".\"simple3886e3439cce68f6363dc8f9d39ce041\" (id timeuuid, shardid bigint, value varchar,index_1 varchar,index_2 varchar, PRIMARY KEY ((shardid, index_1),id) );", result.next().getQuery());
		assertEquals("CREATE TABLE \"functional\".\"simple2849d92a26f695e548ccda0db2a09b00\" (id timeuuid, shardid bigint, value varchar,index_1 varchar,index_2 varchar, PRIMARY KEY ((shardid, index_2),id) );", result.next().getQuery());
		CQLStatement defInsert = result.next();
		assertEquals("INSERT INTO \"rhombus_data\".\"__keyspace_definitions\" (name, def) values (?, ?);", defInsert.getQuery());
		assertEquals("functional", defInsert.getValues()[0]);
		assertEquals("{\"name\":\"functional\",\"replicationClass\":\"SimpleStrategy\",\"consistencyLevel\":\"ONE\",\"replicationFactors\":{\"replication_factor\":1},\"definitions\":[{\"name\":\"customkey\",\"fields\":[{\"name\":\"id\",\"type\":\"varchar\"},{\"name\":\"data1\",\"type\":\"varchar\"}],\"indexes\":[{\"key\":\"data1\",\"shardingStrategy\":{\"type\":\"ShardingStrategyNone\"}}],\"allowNullPrimaryKeyInserts\":true},{\"name\":\"testtype\",\"fields\":[{\"name\":\"newfield\",\"type\":\"varchar\"},{\"name\":\"filtered\",\"type\":\"int\"},{\"name\":\"data1\",\"type\":\"varchar\"},{\"name\":\"data2\",\"type\":\"varchar\"},{\"name\":\"data3\",\"type\":\"varchar\"},{\"name\":\"instance\",\"type\":\"bigint\"},{\"name\":\"type\",\"type\":\"int\"},{\"name\":\"foreignid\",\"type\":\"bigint\"}],\"indexes\":[{\"key\":\"instance:type\",\"shardingStrategy\":{\"type\":\"ShardingStrategyMonthly\"}},{\"key\":\"data1:data2\",\"shardingStrategy\":{\"type\":\"ShardingStrategyNone\"}},{\"key\":\"foreignid:instance:type\",\"shardingStrategy\":{\"type\":\"ShardingStrategyMonthly\"}},{\"key\":\"foreignid\",\"shardingStrategy\":{\"type\":\"ShardingStrategyNone\"}}],\"allowNullPrimaryKeyInserts\":true},{\"name\":\"simple\",\"fields\":[{\"name\":\"value\",\"type\":\"varchar\"},{\"name\":\"index_1\",\"type\":\"varchar\"},{\"name\":\"index_2\",\"type\":\"varchar\"}],\"indexes\":[{\"key\":\"index_1\",\"shardingStrategy\":{\"type\":\"ShardingStrategyNone\"}},{\"key\":\"index_2\",\"shardingStrategy\":{\"type\":\"ShardingStrategyNone\"}}],\"allowNullPrimaryKeyInserts\":true}]}", defInsert.getValues()[1]);

		//That should be it
		assertFalse(result.hasNext());
	}

}
