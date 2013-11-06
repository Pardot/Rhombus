package com.pardot.rhombus;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.util.StringUtil;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * User: Rob Righter
 * Date: 11/5/13
 */
public class StringUtilTest {

	@Test
	public void testDetailedListToString() {
		List list = Lists.newArrayList();
		list.add("zero");
		list.add(null);
		list.add("two");
		list.add(null);

		String actual = StringUtil.detailedListToString(list);
		String expected = "{\n" +
				"\t(java.lang.String) zero\n" +
				"\t(NULL) NULL\n" +
				"\t(java.lang.String) two\n" +
				"\t(NULL) NULL\n" +
				"}";
		assertEquals(expected,actual);
	}

	@Test
	public void testDetailedMapToString() {
		Map<String,String> map = Maps.newHashMap();
		map.put("0","zero");
		map.put("1",null);
		map.put("2","two");
		map.put("3",null);

		String actual = StringUtil.detailedMapToString(map);
		String expected = "{\n" +
				"\t3: (NULL) NULL\n" +
				"\t2: (java.lang.String) two\n" +
				"\t1: (NULL) NULL\n" +
				"\t0: (java.lang.String) zero\n" +
				"}";
		assertEquals(expected,actual);
	}
}
