package com.pardot.rhombus;

import com.google.common.collect.Maps;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CIndex;
import com.pardot.rhombus.helpers.TestHelpers;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by rrighter on 4/21/14.
 */
public class UpdateProcessorTest extends TestCase {

	public void testAreIndexValuesEqual() {
		//test for: protected boolean areIndexValuesEqual(Map<String,Object> a, Map<String,Object> b)
		Subject subject = new Subject(null);

		//test that equal values return true
		Map<String,Object> a = Maps.newHashMap();
		Map<String,Object> b = Maps.newTreeMap();
		a.put("one","test"+1);
		b.put("one", "test" + 1);
		a.put("two", null);
		b.put("two", null);
		a.put("three", Long.valueOf(3L));
		b.put("three", Long.valueOf(3L));
		assertTrue(subject.TESTareIndexValuesEqual(a, b));

		//test that uneqal values return false
		a.put("two","Unequal");
		assertFalse(subject.TESTareIndexValuesEqual(a,b));
	}

	public void testGetListOfEffectedIndexes()throws IOException {
		//test for: protected List<CIndex> getListOfEffectedIndexes(CDefinition def, Map<String,Object> a, Map<String,Object> b)
		Subject subject = new Subject(null);
		String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
		CDefinition def = CDefinition.fromJsonString(json);
		//foreignid:type:instance
		Map<String,Object> a = Maps.newHashMap();
		Map<String,Object> b = Maps.newTreeMap();
		a.put("foreignid", 1L);
		List<CIndex> result = subject.TESTgetListOfEffectedIndexes(def,a,b);
		assertEquals("foreignid:instance:type",result.get(0).getName());
		assertEquals("foreignid",result.get(1).getName());

		a.put("instance", 1L);
		result = subject.TESTgetListOfEffectedIndexes(def,a,b);
		assertEquals("instance:type",result.get(0).getName());
		assertEquals("foreignid:instance:type",result.get(1).getName());
		assertEquals("foreignid",result.get(2).getName());

	}

	public void testAreAnyValuesNull() {
		//test for: protected boolean areAnyValuesNull(Map<String,Object> indexValues){
		Subject subject = new Subject(null);

		//test that equal values return true
		Map<String,Object> a = Maps.newHashMap();
		a.put("one","test"+1);
		a.put("two", null);
		a.put("three", Long.valueOf(3L));
		assertTrue(subject.TESTareAnyValuesNull(a));

		//test that uneqal values return false
		a.put("two","Unequal");
		assertFalse(subject.TESTareAnyValuesNull(a));
	}


	private class Subject extends UpdateProcessor {

		public Subject(ObjectMapper om){
			super(om);
		}

		public boolean TESTareIndexValuesEqual(Map<String,Object> a, Map<String,Object> b)
		{
			return this.areIndexValuesEqual(a, b);
		}

		public List<CIndex> TESTgetListOfEffectedIndexes(CDefinition def, Map<String,Object> a, Map<String,Object> b)
		{
			return this.getListOfEffectedIndexes(def, a, b);
		}

		public boolean TESTareAnyValuesNull(Map<String,Object> indexValues)
		{
			return this.areAnyValuesNull(indexValues);
		}
	}



}


