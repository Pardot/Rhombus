package com.pardot.rhombus;

import com.google.common.base.Joiner;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CField;
import com.pardot.rhombus.cobject.CIndex;
import com.pardot.rhombus.helpers.TestHelpers;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 4/5/13
 */
public class CDefinitionTest extends TestCase{

    public void testFields() throws IOException {
        String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
        CDefinition def = CDefinition.fromJsonString(json);
        Map<String, CField> fields = def.getFields();
        //Make sure the size is correct
        assertEquals(7, fields.size());
        //Check the first field
        CField field = fields.get("foreignid");
        assertEquals("foreignid", field.getName());
        assertEquals(CField.CDataType.BIGINT, field.getType());
    }

	public void testEquals() throws IOException {
		String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
		CDefinition def1 = CDefinition.fromJsonString(json);
		CDefinition def2 = CDefinition.fromJsonString(json);
		assertTrue(def1.equals(def2));
	}

	public void testNotEquals() throws IOException {
		String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
		CDefinition def1 = CDefinition.fromJsonString(json);
		CDefinition def2 = CDefinition.fromJsonString(json);
		def2.setName("Other name");
		assertFalse(def1.equals(def2));
	}

	public void testNotEqualsFields() throws IOException {
		String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
		CDefinition def1 = CDefinition.fromJsonString(json);
		CDefinition def2 = CDefinition.fromJsonString(json);
		Map<String, CField> fields = def2.getFields();
		fields.values().iterator().next().setName("Other name");
		assertFalse(def1.equals(def2));
	}

	public void testGetMostSelectiveMatchingIndexMostSelective() throws IOException {
		String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
		CDefinition definition = CDefinition.fromJsonString(json);
		SortedMap<String, Object> indexValues = new TreeMap<String, Object>();
		indexValues.put("type", "typeValue");
		indexValues.put("foreignid", 13);
		indexValues.put("instance", 11);

		CIndex matchingIndex = definition.getMostSelectiveMatchingIndex(indexValues);
		String expectedIndexKey = Joiner.on(":").join(indexValues.keySet());
		assertEquals(expectedIndexKey, matchingIndex.getKey());
	}

	public void testGetMostSelectiveMatchingIndexMiddle() throws IOException {
		String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
		CDefinition definition = CDefinition.fromJsonString(json);
		SortedMap<String, Object> indexValues = new TreeMap<String, Object>();
		indexValues.put("type", "typeValue");
		indexValues.put("instance", 11);

		CIndex matchingIndex = definition.getMostSelectiveMatchingIndex(indexValues);
		String expectedIndexKey = Joiner.on(":").join(indexValues.keySet());
		assertEquals(expectedIndexKey, matchingIndex.getKey());
	}

	public void testGetMostSelectiveMatchingIndexLeastSelective() throws IOException {
		String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
		CDefinition definition = CDefinition.fromJsonString(json);
		SortedMap<String, Object> indexValues = new TreeMap<String, Object>();
		indexValues.put("foreignid", 13);

		CIndex matchingIndex = definition.getMostSelectiveMatchingIndex(indexValues);
		String expectedIndexKey = Joiner.on(":").join(indexValues.keySet());
		assertEquals(expectedIndexKey, matchingIndex.getKey());
	}

	public void testIsFieldUsedInAnyIndexYes() throws IOException {
		String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
		CDefinition definition = CDefinition.fromJsonString(json);
		assertTrue(definition.isFieldUsedInAnyIndex("foreignid"));
	}

	public void testIsFieldUsedInAnyIndexNo() throws IOException {
		String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
		CDefinition definition = CDefinition.fromJsonString(json);
		assertFalse(definition.isFieldUsedInAnyIndex("filtered"));
	}
}

