package com.pardot.rhombus;

import com.pardot.rhombus.cobject.*;
import com.pardot.rhombus.cobject.migrations.CKeyspaceDefinitionMigrator;
import com.pardot.rhombus.cobject.migrations.CObjectMigrationException;
import com.pardot.rhombus.cobject.shardingstrategy.ShardingStrategyNone;
import com.pardot.rhombus.util.JsonUtil;
import org.junit.Test;

import javax.validation.constraints.AssertTrue;

import static org.junit.Assert.*;

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

		CKeyspaceDefinitionMigrator subject = new CKeyspaceDefinitionMigrator(OldKeyspaceDefinition, NewKeyspaceDefinition);
		assertTrue(subject.isMigratable());

		//now try adding an invalid Object
		NewKeyspaceDefinition = JsonUtil.objectFromJsonResource(CKeyspaceDefinition.class, this.getClass().getClassLoader(), "CKeyspaceTestData.js");
		CField newField = new CField("newfield", CField.CDataType.VARCHAR);
		NewKeyspaceDefinition.getDefinitions().get("testtype").getFields().put(newField.getName(),newField);
		subject = new CKeyspaceDefinitionMigrator(OldKeyspaceDefinition, NewKeyspaceDefinition);
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
		//add new object
		CDefinition NewObjectDefinition = JsonUtil.objectFromJsonResource(CDefinition.class, this.getClass().getClassLoader(), "MigrationTestCDefinition.js");
		NewKeyspaceDefinition.getDefinitions().put(NewObjectDefinition.getName(),NewObjectDefinition);

		//Construct the object. It should be migratable
		CKeyspaceDefinitionMigrator subject = new CKeyspaceDefinitionMigrator(OldKeyspaceDefinition, NewKeyspaceDefinition);
		assertTrue(subject.isMigratable());

		//Now verify that the correct CQL is generated for the migration
		CQLStatementIterator result = subject.getMigrationCQL();
		assertEquals("CREATE TABLE \"testtypeb4e47a87138afd20159a6522134a3bc2\" (id timeuuid, shardid bigint, filtered int,data1 varchar,data2 varchar,data3 varchar,instance bigint,type int,foreignid bigint, PRIMARY KEY ((shardid, data1, data2),id) );",result.next().getQuery());
		assertEquals("CREATE TABLE \"simple\" (id timeuuid PRIMARY KEY, value varchar,index_1 varchar,index_2 varchar);", result.next().getQuery());
		assertEquals("CREATE TABLE \"simple3886e3439cce68f6363dc8f9d39ce041\" (id timeuuid, shardid bigint, value varchar,index_1 varchar,index_2 varchar, PRIMARY KEY ((shardid, index_1),id) );", result.next().getQuery());
		assertEquals("CREATE TABLE \"simple2849d92a26f695e548ccda0db2a09b00\" (id timeuuid, shardid bigint, value varchar,index_1 varchar,index_2 varchar, PRIMARY KEY ((shardid, index_2),id) );", result.next().getQuery());
		CQLStatement defInsert = result.next();
		assertEquals("INSERT INTO functional.\"__keyspace_definitions\" (id, shardid, def) values (?, ?, ?);", defInsert.getQuery());
		assertEquals( 1L, defInsert.getValues()[1]);
		assertEquals("{\"name\":\"functional\",\"replicationClass\":\"SimpleStrategy\",\"consistencyLevel\":\"ONE\",\"replicationFactors\":{\"replication_factor\":1},\"definitions\":[{\"name\":\"testtype\",\"fields\":[{\"name\":\"filtered\",\"type\":\"int\"},{\"name\":\"data1\",\"type\":\"varchar\"},{\"name\":\"data2\",\"type\":\"varchar\"},{\"name\":\"data3\",\"type\":\"varchar\"},{\"name\":\"instance\",\"type\":\"bigint\"},{\"name\":\"type\",\"type\":\"int\"},{\"name\":\"foreignid\",\"type\":\"bigint\"}],\"indexes\":[{\"key\":\"instance:type\",\"shardingStrategy\":{\"type\":\"ShardingStrategyMonthly\"}},{\"key\":\"data1:data2\",\"shardingStrategy\":{\"type\":\"ShardingStrategyNone\"}},{\"key\":\"foreignid:instance:type\",\"shardingStrategy\":{\"type\":\"ShardingStrategyMonthly\"}},{\"key\":\"foreignid\",\"shardingStrategy\":{\"type\":\"ShardingStrategyNone\"}}],\"allowNullPrimaryKeyInserts\":true},{\"name\":\"simple\",\"fields\":[{\"name\":\"value\",\"type\":\"varchar\"},{\"name\":\"index_1\",\"type\":\"varchar\"},{\"name\":\"index_2\",\"type\":\"varchar\"}],\"indexes\":[{\"key\":\"index_1\",\"shardingStrategy\":{\"type\":\"ShardingStrategyNone\"}},{\"key\":\"index_2\",\"shardingStrategy\":{\"type\":\"ShardingStrategyNone\"}}],\"allowNullPrimaryKeyInserts\":true}]}", defInsert.getValues()[2]);

		//That should be it
		assertFalse(result.hasNext());
	}

}
