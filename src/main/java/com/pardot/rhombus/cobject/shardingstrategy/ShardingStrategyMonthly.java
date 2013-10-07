package com.pardot.rhombus.cobject.shardingstrategy;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 4/13/13
 */
public class ShardingStrategyMonthly extends TimebasedShardingStrategy {

	public ShardingStrategyMonthly(){
	}

	public long getShardKey(long timestamp){
		DateTime d = new DateTime(timestamp, DateTimeZone.UTC);
		long year = (long)d.getYear();
		long month = (long)d.getMonthOfYear();
		return this.offset + ((year - START_YEAR)*12)+month;
	}
}
