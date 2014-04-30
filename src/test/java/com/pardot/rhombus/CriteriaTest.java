package com.pardot.rhombus;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.*;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 5/11/13
 */
public class CriteriaTest {
	private static final Logger logger = LoggerFactory.getLogger(CriteriaTest.class);

	@Test
	public void testToString() {
		Criteria criteria = new Criteria();
		SortedMap<String, Object> indexKeys = new TreeMap<String, Object>();
		indexKeys.put("account_id", "3");
		indexKeys.put("object_type", "account");
		indexKeys.put("object_id", "3");
		criteria.setIndexKeys(indexKeys);
		criteria.setStartTimestamp(System.currentTimeMillis() - 3600000);
		criteria.setLimit(50L);
		criteria.setOrdering("ASC");

		logger.debug(criteria.toString());
		assertNotNull(criteria.toString());
	}
}
