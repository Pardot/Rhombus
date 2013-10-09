package com.pardot.rhombus;

import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CField;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.cobject.migrations.CKeyspaceDefinitionMigrator;
import com.pardot.rhombus.cobject.migrations.CObjectMigrationException;
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
		//todo: write tests for this
		assertFalse(true);
	}

}
