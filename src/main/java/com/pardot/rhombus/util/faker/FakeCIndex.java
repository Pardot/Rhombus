package com.pardot.rhombus.util.faker;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CIndex;
import com.pardot.rhombus.cobject.shardingstrategy.*;

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
		uniqueRange = new FakeIdRange(def.getPrimaryKeyCDataType(),startId,totalObjects,objectsPerShard,index.getShardingStrategy());
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



}
