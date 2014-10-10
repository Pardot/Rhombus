package com.pardot.rhombus.util.faker;

import com.google.common.collect.Lists;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.RhombusException;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;
import com.pardot.rhombus.cobject.CObjectOrdering;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * User: Rob Righter
 * Date: 3/13/14
 */
public class FakeR {
	protected CKeyspaceDefinition keyspace;
	protected List<FakeCDefinition> fakeDefinitions;

	public FakeR(CKeyspaceDefinition keyspace, Long totalWideRowsPerIndex, Long totalObjectsPerWideRange, Long objectsPerShard) throws RhombusException {
		this.keyspace = keyspace;

		Map<String, CDefinition> definitions = keyspace.getDefinitions();
		this.fakeDefinitions = Lists.newArrayList();
		for (String defKey : definitions.keySet()) {
			this.fakeDefinitions.add(new FakeCDefinition(definitions.get(defKey), totalWideRowsPerIndex, totalObjectsPerWideRange, objectsPerShard));
		}
	}

	/**
	 *
	 * @param startId - The range of the minimumUniqueIndexHits counter. Set to null in order to
	 *                         iterate over all values.
	 */
	public Iterator<Map<String, Object>> getMasterIterator(final CObjectOrdering ordering, final UUID startId, final UUID endId) {
		final Iterator<FakeCDefinition> fakeDefIterator = this.fakeDefinitions.iterator();
		return new Iterator<Map<String, Object>>() {
			Iterator<Map<String, Object>> currentDefinitionIterator = null;

			@Override
			public boolean hasNext() {
				boolean doesHaveNext = false;
				while (!doesHaveNext) {
					if (currentDefinitionIterator == null || !currentDefinitionIterator.hasNext()) { //if we need to move on to the next definition
						if (fakeDefIterator.hasNext()) {//if we have a next definition
							try {
								currentDefinitionIterator = fakeDefIterator.next().getIterator(ordering, startId, endId);
							} catch (RhombusException re) {
								System.out.print("Error while creating iterator: " + re.getMessage());
								re.printStackTrace();
								throw new RuntimeException("Rhombus Exception");
							}
						} else {
							return false; //we are out of indexes to iterate over
						}
					}
					//set the doesHaveNext from the current index and continue looping just in case we get a false back.
					doesHaveNext = currentDefinitionIterator.hasNext();
					//if doesHaveNext is true, we will break out of the loop, otherwise we keep iterating until we run out of stuff
				}
				return true;
			}

			@Override
			public Map<String, Object> next() {
				return currentDefinitionIterator.next();
			}

			@Override
			public void remove() {
				//no op
			}
		};
	}

	/**
	 *
	 * @param objectType Type of object to get
	 * @param key Key of object to get
	 * @return Object of type with key or null if it does not exist
	 */
	public Map<String, Object> getByKey(String objectType, Object key) {
//		for (FakeCDefinition def : this.fakeDefinitions) {
//			if (def.cdef.getName().equals(objectType)) {
//				def.cdef.ge
//			}
//		}

		return null;
	}

	public Iterator<Map<String, Object>> list(String objectType, Criteria criteria) {
		return null;
	}

}
