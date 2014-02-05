package com.pardot.rhombus;

import com.google.common.collect.Maps;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 5/11/13
 */
public class ObjectMapperTest {
	private static final Logger logger = LoggerFactory.getLogger(ObjectMapperTest.class);

	public ObjectMapper getObjectMapper() {
		CKeyspaceDefinition definition = mock(CKeyspaceDefinition.class);
		ObjectMapper objectMapper = new ObjectMapper(null, definition, 1000, 1000l);
		return  objectMapper;
	}

	@Test
	public void testResultMatchesFilterMatch() {
		Map<String, Object> result = Maps.newHashMap();
		result.put("account_id", 2);
		result.put("is_filtered", false);
		Map<String, Object> clientFilters = Maps.newHashMap();
		clientFilters.put("is_filtered", false);

		boolean matches = getObjectMapper().resultMatchesFilters(result, clientFilters);
		assertTrue(matches);
	}

	@Test
	public void testResultMatchesFilterMiss() {
		Map<String, Object> result = Maps.newHashMap();
		result.put("account_id", 2);
		result.put("is_filtered", true);
		Map<String, Object> clientFilters = Maps.newHashMap();
		clientFilters.put("is_filtered", false);

		boolean matches = getObjectMapper().resultMatchesFilters(result, clientFilters);
		assertFalse(matches);
	}
}
