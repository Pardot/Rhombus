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
	public FakeRIterator getMasterIterator(final CObjectOrdering ordering, final UUID startId, final UUID endId) {
		final Iterator<FakeCDefinition> fakeDefIterator = this.fakeDefinitions.iterator();
		return new FakeRIterator(fakeDefIterator, ordering, startId, endId);
	}

	public Iterator<Map<String, Object>> list(String objectType, Criteria criteria) {
		return null;
	}

	public class FakeRIterator implements Iterator<Map<String, Object>> {
		private final Iterator<FakeCDefinition> fakeDefIterator;
		private final CObjectOrdering ordering;
		private final UUID startId;
		private final UUID endId;
		Iterator<Map<String, Object>> currentDefinitionIterator;
		String currentObjectType;

		public FakeRIterator(Iterator<FakeCDefinition> fakeDefIterator, CObjectOrdering ordering, UUID startId, UUID endId) {
			this.fakeDefIterator = fakeDefIterator;
			this.ordering = ordering;
			this.startId = startId;
			this.endId = endId;
			currentDefinitionIterator = null;
			currentObjectType = null;
		}

		@Override
		public boolean hasNext() {
			boolean doesHaveNext = false;
			while (!doesHaveNext) {
				if (currentDefinitionIterator == null || !currentDefinitionIterator.hasNext()) { //if we need to move on to the next definition
					if (fakeDefIterator.hasNext()) {//if we have a next definition
						try {
							FakeCDefinition nextDef = fakeDefIterator.next();
							currentDefinitionIterator = nextDef.getIterator(ordering, startId, endId);
							this.currentObjectType = nextDef.getCdef().getName();
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

		public String getCurrentObjectType() {
			return this.currentObjectType;
		}
	}
}
