package com.pardot.rhombus.util.faker;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.pardot.rhombus.Criteria;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.RhombusException;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CIndex;
import com.pardot.rhombus.cobject.CObjectOrdering;
import com.pardot.rhombus.cobject.shardingstrategy.*;

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
	private List<FakeIdRange> coveredRanges = null;

	public FakeCIndex(CIndex index)
	{
		this.index = index;
		coveredRanges = Lists.newArrayList();
	}

	public boolean isCovering(CIndex otherIndex)
	{
		return this.index.getKey().contains(otherIndex.getKey());
	}

	public void compileIdRange(CDefinition def, Object startId, Long totalObjects, Long objectsPerShard)
	{
		uniqueRange = new FakeIdRange(def.getPrimaryKeyCDataType(),startId,totalObjects,objectsPerShard,index.getShardingStrategy(), index.getKey());
	}

	public FakeIdRange getUniqueRange() {
		return uniqueRange;
	}

	public void addCoveredRange(FakeIdRange range) {
		coveredRanges.add(range);
	}

	public Map<String, Object> makeObject(FakeIdRange.IdInRange idInRange){
		Map<String,Object> ret = Maps.newHashMap();

		return ret;
	}

	/**
	 *
	 * @return
	 */
	public Iterator<Map<String, Object>> getMasterIterator(CObjectOrdering ordering){
		return new FakeCIndexIterator(this.getUniqueRange().getIterator(ordering));
	}

	public Iterator<Map<String, Object>> getIterator(CObjectOrdering ordering, Object startId, Object endId) throws RhombusException {
		return new FakeCIndexIterator(this.getUniqueRange().getIterator(ordering,startId,endId));
	}

	/**
	 *
	 * @param objectType Type of object to get
	 * @param key Key of object to get
	 * @return Object of type with key or null if it does not exist
	 */
	public Map<String, Object> getByKey(String objectType, Object key) throws RhombusException {
		Map<String,Object> ret = null;
		if(uniqueRange.isIdInRange(key)){
			return makeObject(uniqueRange.getIdInRangeAtCounter(uniqueRange.getCounterAtId(key)));
		}
		else {
			for(FakeIdRange fr : this.coveredRanges){
				if(fr.isIdInRange(key)){
					return makeObject(fr.getIdInRangeAtCounter(uniqueRange.getCounterAtId(key)));
				}
			}
		}
		return null;
	}

	public Iterator<Map<String, Object>> list(String objectType, Criteria criteria) {
		//TODO: get the counter range for this criteria and make an iterator for it
		return null;
	}

	public class FakeCIndexIterator implements Iterator<Map<String,Object>> {

		private Iterator<FakeIdRange.IdInRange> it;

		public FakeCIndexIterator(Iterator<FakeIdRange.IdInRange> it){
			this.it = it;
		}

		public boolean hasNext(){
			return it.hasNext();
		}

		public Map<String,Object> next() {
			return makeObject(it.next());
		}

		public void remove(){
			it.remove();
		}

	}



}
