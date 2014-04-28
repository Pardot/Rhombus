package com.pardot.rhombus;

import com.datastax.driver.core.utils.UUIDs;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CObjectOrdering;
import com.pardot.rhombus.helpers.TestHelpers;
import com.pardot.rhombus.util.faker.FakeCDefinition;
import com.pardot.rhombus.util.faker.FakeCIndex;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Created by rrighter on 4/28/14.
 */
public class FakeCDefTest {

	@Test
	public void getMasterIteratorTest() throws Exception {

		String json = TestHelpers.readFileToString(this.getClass(), "CObjectCQLGeneratorTestData.js");
		CDefinition def = CDefinition.fromJsonString(json);

		FakeCDefinition subject = new FakeCDefinition(def,100L,50L, 10L);
		Iterator<Map<String,Object>> masterIt = subject.getMasterIterator(CObjectOrdering.DESCENDING);

		long counter = 0;
		while(masterIt.hasNext()){
			counter++;
			Map<String,Object> item = masterIt.next();
			System.out.println(item);
		}

		assertEquals(3L*5000L,counter);

	}
}
