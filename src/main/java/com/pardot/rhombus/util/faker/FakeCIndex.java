package com.pardot.rhombus.util.faker;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.*;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.RhombusException;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CField;
import com.pardot.rhombus.cobject.CIndex;
import com.pardot.rhombus.cobject.CObjectOrdering;
import com.pardot.rhombus.cobject.shardingstrategy.*;

import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * User: Rob Righter
 * Date: 3/13/14
 */
public class FakeCIndex {

	private CIndex index;
	private FakeIdRange uniqueRange = null;
	private List<FakeCIndex> indexesThatIAmASubsetOf = null;
	private Range<Long> wideRowCounterRange;
	private List<String> nonIndexValuesOnAnyIndex;
	private CDefinition def;

	public FakeCIndex(CIndex index,
	                  CDefinition def,
	                  Object startId,
	                  Long totalWideRows,
	                  Long totalObjectsPerWideRange,
	                  Long objectsPerShard ){
		this.index = index;
		this.indexesThatIAmASubsetOf = Lists.newArrayList();
		// Each index's id range is represented by a fakeIdRange object, which represents all the ids for that index,
		// across all the index's shards/wide rows
		this.uniqueRange = new FakeIdRange(def.getPrimaryKeyCDataType(),startId,totalObjectsPerWideRange,objectsPerShard,index.getShardingStrategy(), index.getKey());
		this.wideRowCounterRange = Range.closed(1L, totalWideRows);
		this.def = def;

		this.nonIndexValuesOnAnyIndex = Lists.newArrayList();
		List<String> masterIndexValuesList = Lists.newArrayList();
		for(CIndex i : def.getIndexes().values()){
			masterIndexValuesList.addAll(i.getCompositeKeyList());
		}

		for(CField f : def.getFields().values()){
			if(!masterIndexValuesList.contains(f.getName())){
				this.nonIndexValuesOnAnyIndex.add(f.getName());
			}
		}





	}

	public boolean isCovering(CIndex otherIndex)
	{
		return this.index.getKey().contains(otherIndex.getKey());
	}

	public FakeIdRange getUniqueRange() {
		return uniqueRange;
	}

	public void addCoveringIndex(FakeCIndex findex) {
		indexesThatIAmASubsetOf.add(findex);
	}

	public Map<String, Object> makeObject(Long topCounter, FakeIdRange.IdInRange idInRange){
		Map<String,Object> ret = Maps.newHashMap();
		//set the Id
		ret.put("id", idInRange.getId());

		//set the index values
		for(String indexValue : index.getCompositeKeyList()){
			CField f = def.getField(indexValue);
			ret.put(indexValue,getFieldValueAtCounter(topCounter,f));
		}

		//set the non-index values
		for(String nonIndexValue : this.nonIndexValuesOnAnyIndex){
			CField f = def.getField(nonIndexValue);
			ret.put(nonIndexValue,getFieldValueAtCounter(idInRange.getCounterValue(),f));
		}
		return ret;
	}

	public Object getSuggestedIdForStartofNextIndex(TimebasedShardingStrategy shardingStrategyOfNextIndex) throws RhombusException {
		return this.getUniqueRange().getSuggestedIdForStartofNextIndex(shardingStrategyOfNextIndex);
	}

	/**
	 *
	 * @return
	 */
	public Iterator<Map<String, Object>> getMasterIterator(CObjectOrdering ordering) throws RhombusException{
		return getIterator(ordering, null, null);
	}

	public Iterator<Map<String, Object>> getIterator(CObjectOrdering ordering, Object startId, Object endId) throws RhombusException {
		ContiguousSet<Long> wideRowCounterSet = ContiguousSet.create(this.wideRowCounterRange, DiscreteDomain.longs());
		Iterator<Long> topLevelWideRowIterator = (ordering == CObjectOrdering.ASCENDING) ? wideRowCounterSet.iterator() : wideRowCounterSet.descendingIterator();
		return new FakeCIndexIterator(topLevelWideRowIterator, this.getUniqueRange(), ordering, startId, endId);
	}

	/**
	 *
	 * @param key Key of object to get
	 * @return Object of type with key or null if it does not exist
	 */
	public Map<String, Object> getByKeyAndCounter(Long topLevelCounter, Object key) throws RhombusException {
		Map<String,Object> ret = null;
		if(uniqueRange.isIdInRange(key)){
			return makeObject(topLevelCounter, uniqueRange.getIdInRangeAtCounter(uniqueRange.getCounterAtId(key)));
		}
		else {
			for(FakeCIndex fr : this.indexesThatIAmASubsetOf){
				if(fr.getUniqueRange().isIdInRange(key)){
					return makeObject(topLevelCounter, fr.getUniqueRange().getIdInRangeAtCounter(uniqueRange.getCounterAtId(key)));
				}
			}
		}
		return null;
	}

	public Iterator<Map<String, Object>> list(String objectType, Criteria criteria) {
		//TODO: get the counter range for this criteria and make an iterator for it
		return null;
	}

	public Object getFieldValueAtCounter(Long counter, CField field){
		switch (field.getType()) {
			case ASCII:
			case VARCHAR:
			case TEXT:
				return counter+"";
			case INT:
				return Integer.valueOf(counter.intValue());
			case BIGINT:
			case COUNTER:
				return Long.valueOf(counter);
			case BLOB:
				throw new IllegalArgumentException();
			case BOOLEAN:
				return ((counter%2)==0)? Boolean.valueOf(true) : Boolean.valueOf(false);
			case DECIMAL:
			case FLOAT:
				return Float.valueOf(counter.floatValue());
			case DOUBLE:
				return Double.valueOf(counter.floatValue());
			case TIMESTAMP:
				return new Date(counter);
			case UUID:
			case TIMEUUID:
				return UUIDs.startOf(counter);
			case VARINT:
				return BigInteger.valueOf(counter);
			default:
				return null;
		}

	}

	public class FakeCIndexIterator implements Iterator<Map<String,Object>> {

		private FakeIdRange fRange;
		private Iterator<FakeIdRange.IdInRange> rowIt;
		private Iterator<Long> wideRowCounterIt;
		private CObjectOrdering ordering;
		Object startId;
		Object endId;
		Long currentWideRowCounter;


		public FakeCIndexIterator(Iterator<Long> wideRowCounterIt, FakeIdRange fRange, CObjectOrdering ordering, Object startId, Object endId) throws RhombusException{
			this.fRange = fRange;
			this.ordering = ordering;
			this.startId = startId;
			this.endId = endId;
			resetRowIt();
			this.wideRowCounterIt = wideRowCounterIt;
			this.currentWideRowCounter = wideRowCounterIt.next();
		}

		public void resetRowIt() throws RhombusException {
			if((startId != null) && (endId != null)){
				this.rowIt = fRange.getIterator(ordering,startId,endId);
			}
			else{
				this.rowIt = fRange.getIterator(ordering);
			}
		}

		public boolean hasNext(){
			return rowIt.hasNext();
		}

		public Map<String,Object> next() {
			if(rowIt.hasNext()){
				FakeIdRange.IdInRange lastIdInRange = rowIt.next();
				return makeObject(lastIdInRange.getCounterValue(),lastIdInRange);
			}
			else {
				return null;
			}
		}

		public void remove(){
			wideRowCounterIt.remove();
			rowIt.remove();
		}

	}



}
