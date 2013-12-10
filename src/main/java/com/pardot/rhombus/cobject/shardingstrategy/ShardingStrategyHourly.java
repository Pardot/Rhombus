package com.pardot.rhombus.cobject.shardingstrategy;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Hours;

public class ShardingStrategyHourly extends TimebasedShardingStrategy {

    public ShardingStrategyHourly(){
    }

    public long getShardKey(long timestamp){
        DateTime d = new DateTime(timestamp, DateTimeZone.UTC);
        DateTime start = new DateTime(2000, 1, 1, 0, 0, 0, 0, DateTimeZone.UTC);
        int daysSinceEpoch = Hours.hoursBetween(start, d).getHours();
        return this.offset + daysSinceEpoch;
    }
}
