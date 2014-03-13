package com.pardot.rhombus.util.faker;

import com.google.common.collect.Lists;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CIndex;
import com.pardot.rhombus.cobject.shardingstrategy.*;

import java.util.List;

/**
 * User: Rob Righter
 * Date: 3/13/14
 */
public class FakeCIndex {

	private CIndex index;
	private List<FakeIdRange> idRangeList = null;

	public FakeCIndex(CIndex index)
	{
		this.index = index;
		idRangeList = Lists.newArrayList();
	}

	public boolean isCovering(CIndex otherIndex)
	{
		return this.index.getKey().contains(otherIndex.getKey());
	}

	public void compileIdRange(CDefinition def, Object startId, Long totalObjects, Long objectsPerShard)
	{
		FakeIdRange first = new FakeIdRange(def.getPrimaryKeyCDataType(),startId,totalObjects,objectsPerShard,index.getShardingStrategy());
		idRangeList.add(first);

	}

	public FakeIdRange getIdRange() {
		return idRange;
	}



	public Long getStartOfNextShard(Long current)
	{
		return 1L;
	}

	public Long getSuggestedStartForNextIndex(){
		return getStartOfNextShard(idRange.getEndTimeStamp()+1);
	}



}
