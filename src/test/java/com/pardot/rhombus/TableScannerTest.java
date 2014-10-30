package com.pardot.rhombus;

import com.pardot.rhombus.cobject.CObjectTokenVisitorFactory;
import junit.framework.TestCase;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 5/6/13
 */
public class TableScannerTest extends TestCase {

	public void testSimpleRange() {
		TableScanner scanner = new TableScanner(mockObjectMapper(), "testtype", 2, mockVisitorFactory(), null);
		List<Map.Entry<Long, Long>> ranges = scanner.makeRanges();
		assertEquals(2, ranges.size());
		assertEquals(Long.valueOf(Long.MIN_VALUE), ranges.get(0).getKey());
		assertEquals(Long.valueOf(-1l), ranges.get(0).getValue());
		assertEquals(Long.valueOf(0), ranges.get(1).getKey());
		assertEquals(Long.valueOf(Long.MAX_VALUE), ranges.get(1).getValue());
	}

	public void testRanges() {
		for(int i = 1 ; i < 17 ; i++) {
			TableScanner scanner = new TableScanner(mockObjectMapper(), "testtype", i, mockVisitorFactory(), null);
			List<Map.Entry<Long, Long>> ranges = scanner.makeRanges();
			assertEquals(i, ranges.size());
			assertEquals(Long.valueOf(Long.MIN_VALUE), ranges.get(0).getKey());
			Long lastMax = ranges.get(0).getValue();
			for(int j = 1 ; j < i ; j++) {
				Map.Entry<Long, Long> range = ranges.get(j);
				assertEquals(Long.valueOf(lastMax + 1l), range.getKey());
				lastMax = range.getValue();
			}
			assertEquals(Long.valueOf(Long.MAX_VALUE), ranges.get(ranges.size()-1).getValue());
		}
	}

	private ObjectMapper mockObjectMapper() {
		return mock(ObjectMapper.class);
	}

	private CObjectTokenVisitorFactory mockVisitorFactory() {
		return mock(CObjectTokenVisitorFactory.class);
	}

}
