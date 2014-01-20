package com.pardot.rhombus;

import com.pardot.rhombus.cobject.CField;
import com.pardot.rhombus.cobject.CIndex;
import com.pardot.rhombus.cobject.shardingstrategy.ShardingStrategyDaily;
import com.pardot.rhombus.cobject.shardingstrategy.ShardingStrategyMonthly;
import junit.framework.TestCase;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 4/5/13
 */
public class CIndexTest extends TestCase{

	public void testEquals() {
		CIndex index1 = new CIndex("field1:field2:field3", new ShardingStrategyDaily());
		CIndex index2 = new CIndex("field1:field2:field3", new ShardingStrategyDaily());
		assertTrue(index1.equals(index2));
	}

	public void testNotEqualsKey() {
		CIndex index1 = new CIndex("field1:field2:field3", new ShardingStrategyDaily());
		CIndex index2 = new CIndex("field1:field2:field4", new ShardingStrategyDaily());
		assertFalse(index1.equals(index2));
	}

	public void testNotEqualsShardingStrategy() {
		CIndex index1 = new CIndex("field1:field2:field3", new ShardingStrategyDaily());
		CIndex index2 = new CIndex("field1:field2:field3", new ShardingStrategyMonthly());
		assertFalse(index1.equals(index2));
	}
}

