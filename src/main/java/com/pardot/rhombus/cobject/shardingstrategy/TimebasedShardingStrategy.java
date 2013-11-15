package com.pardot.rhombus.cobject.shardingstrategy;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Range;
import org.joda.time.DateTime;

import java.util.UUID;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 4/13/13
 */

@JsonTypeInfo(
		use = JsonTypeInfo.Id.NAME,
		include = JsonTypeInfo.As.PROPERTY,
		property = "type")
@JsonSubTypes({
		@JsonSubTypes.Type(value = ShardingStrategyDaily.class, name = "ShardingStrategyDaily"),
		@JsonSubTypes.Type(value = ShardingStrategyWeekly.class, name = "ShardingStrategyWeekly"),
		@JsonSubTypes.Type(value = ShardingStrategyMonthly.class, name = "ShardingStrategyMonthly"),
		@JsonSubTypes.Type(value = ShardingStrategyNone.class, name = "ShardingStrategyNone")
})
public abstract class TimebasedShardingStrategy {


	public static long START_YEAR = 2000;

	@JsonIgnore
	protected long offset = 0;

	public TimebasedShardingStrategy(){
	}

	public TimebasedShardingStrategy(long offset){
		this.offset = offset;
	}

	@JsonIgnore
	public long getShardKey(Object key){
		if(key instanceof UUID){
			return getShardKey(UUIDs.unixTimestamp((UUID)key));
		}
		else if(key instanceof Long){
			return this.getShardKey(((Long)key).longValue());
		}
		else{
			return 1L;
		}
	}

	@JsonIgnore
	public abstract long getShardKey(long timestamp);

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	@JsonIgnore
	public Range<Long> getShardKeyRange(Long timestampStart, Long timestampEnd) throws ShardStrategyException {

		if(timestampStart == null){
			//unbounded start
			if(timestampEnd == null){
				//unbounded start and unbounded end
				//THIS IS NOT ALLOWED. Throw an exception here
				throw new ShardStrategyException("Time range must have either an upper or lower bound");
			}
			else{
				//unbounded start and bounded end
				//never allow shardId to be less than 1
				return Range.closed(1L,getShardKey(Long.valueOf(timestampEnd)));
			}
		}
		else{
			long start = getShardKey(Long.valueOf(timestampStart));
			//bounded start
			if(timestampEnd == null){
				//bounded start and unbounded end
				//never allow the shardid to be greater than now.
				return Range.closed(start, this.getShardKey(DateTime.now().getMillis()));
			}
			else{
				long end = getShardKey(Long.valueOf(timestampEnd));
				//bounded start and bounded end
				return Range.closed(start,end);
			}
		}
	}
}
