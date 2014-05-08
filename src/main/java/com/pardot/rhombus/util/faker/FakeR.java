package com.pardot.rhombus.util.faker;

import com.pardot.rhombus.Criteria;
import com.google.common.collect.Range;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * User: Rob Righter
 * Date: 3/13/14
 */
public class FakeR {

	public FakeR(CKeyspaceDefinition keyspace, Long minimumUniqueIndexHits, Long countPerIndexShard){

	}

	/**
	 *
	 * @param sliceOfIndexHits - The range of the minimumUnqueIndexHits counter. Set to null in order to
	 *                         iterate over all values.
	 * @return
	 */
	Iterator<Map<String, Object>> getMasterIterator(Range<Long> sliceOfIndexHits){
		return null;
	}

	/**
	 *
	 * @param objectType Type of object to get
	 * @param key Key of object to get
	 * @return Object of type with key or null if it does not exist
	 */
	public Map<String, Object> getByKey(String objectType, Object key) {
		return null;
	}

	public Iterator<Map<String, Object>> list(String objectType, Criteria criteria) {
		return null;
	}

}
