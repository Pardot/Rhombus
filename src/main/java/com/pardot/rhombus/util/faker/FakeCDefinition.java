package com.pardot.rhombus.util.faker;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;
import com.pardot.rhombus.RhombusException;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CIndex;
import com.pardot.rhombus.cobject.CObjectOrdering;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * User: Rob Righter
 * Date: 3/13/14
 */
public class FakeCDefinition {

	private CDefinition cdef;
	private Long totalWideRowsPerIndex;
	private Long totalObjectsPerWideRange;
	private Long objectsPerShard;
	private List<FakeCIndex> fakeCIndexes;

	public FakeCDefinition(CDefinition def, Long totalWideRowsPerIndex,
							Long totalObjectsPerWideRange,
							Long objectsPerShard) throws RhombusException
	{
		this.cdef = def;
		this.totalWideRowsPerIndex = totalWideRowsPerIndex;
		this.totalObjectsPerWideRange = totalObjectsPerWideRange;
		this.objectsPerShard = objectsPerShard;
		this.buildFakeIndexes();
	}

	public List<FakeCIndex> buildFakeIndexes() throws RhombusException {
		this.fakeCIndexes = Lists.newArrayList();
		FakeCIndex lastIndex = null;
		for(CIndex i: this.cdef.getIndexesAsList()){
			Object startingId = (lastIndex==null) ? UUIDs.timeBased() : lastIndex.getSuggestedIdForStartofNextIndex(i.getShardingStrategy());
			FakeCIndex toadd = new FakeCIndex(
					i,
					cdef,
					startingId,
					this.totalWideRowsPerIndex,
					this.totalObjectsPerWideRange,
					this.objectsPerShard );
			this.fakeCIndexes.add(toadd);
			lastIndex = toadd;
		}
		return this.fakeCIndexes;
	}

	public CDefinition getCdef() {
		return cdef;
	}

	public Iterator<Map<String, Object>> getIterator(final CObjectOrdering ordering, final Object startId, final Object endId) throws RhombusException {
		final Iterator<FakeCIndex> indexIterator = this.fakeCIndexes.iterator();

		return new Iterator<Map<String, Object>>() {
			Iterator<Map<String, Object>> currentIndexIterator = null;
			@Override
			public boolean hasNext() {
				boolean doesHaveNext = false;
				while(!doesHaveNext){
					if(currentIndexIterator == null || !currentIndexIterator.hasNext()){ //if we need to move on to the next index
						if(indexIterator.hasNext()){//if we have a next index
							try{
								currentIndexIterator = indexIterator.next().getIterator(ordering, startId, endId);
							}
							catch(RhombusException re){
								System.out.print("Error while creating iterator: "+re.getMessage());
								re.printStackTrace();
								throw new RuntimeException("Rhombus Exception");
							}
						}
						else{
							return false; //we are out of indexes to iterate over
						}
					}
					//set the doesHaveNext from the current index and continue looping just in case we get a false back.
					doesHaveNext = currentIndexIterator.hasNext();
					//if doesHaveNext is true, we will break out of the loop, otherwise we keep iterating until we run out of stuff
				}
				return true;
			}

			@Override
			public Map<String, Object> next() {
				return currentIndexIterator.next();
			}

			@Override
			public void remove() {
				//no op
			}
		};

	}

	public Iterator<Map<String, Object>> getMasterIterator(final CObjectOrdering ordering) throws RhombusException{
		return getIterator(ordering, null, null);
	}


}
