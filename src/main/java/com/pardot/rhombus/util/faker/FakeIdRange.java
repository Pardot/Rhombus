package com.pardot.rhombus.util.faker;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.RhombusException;
import com.pardot.rhombus.cobject.CField;
import com.pardot.rhombus.cobject.CObjectOrdering;
import com.pardot.rhombus.cobject.shardingstrategy.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.omg.CosNaming._NamingContextExtStub;

import java.util.*;

/**
 * User: Rob Righter
 * Date: 3/13/14
 */
public class FakeIdRange {

	public static long PARDOT_EPOCH = 946740000000L;

	private Range<Long> counterRange;
	private long spacing;
	private TimebasedShardingStrategy shardingStrategy;
	private CField.CDataType idType;
	private Long objectsPerShard;
	private Long startingShardNumber;

	public FakeIdRange(CField.CDataType idType, Object startAfterId, Long totalObjects, Long objectsPerShard, TimebasedShardingStrategy shardingStrategy){
		this.shardingStrategy = shardingStrategy;
		this.startingShardNumber=shardingStrategy.getShardKey(startAfterId)+1;
		this.objectsPerShard = objectsPerShard;
		this.spacing = getSpacingForShardingStrategy(objectsPerShard);
		this.idType = idType;
		this.counterRange = Range.closed(1L, totalObjects);
	}

	public Iterator<IdInRange> getIterator(CObjectOrdering ordering) {
		return getIterator(ordering, counterRange);
	}

	private Iterator<IdInRange> getIterator(CObjectOrdering ordering, Range<Long> range) {
		ContiguousSet<Long> set = ContiguousSet.create(range, DiscreteDomain.longs());
		Iterator<Long> rangeIterator = (ordering == CObjectOrdering.ASCENDING) ? set.iterator() : set.descendingIterator();
		return new IdIterator(rangeIterator);
	}

	public Iterator<IdInRange> getIterator(CObjectOrdering ordering, Object startId, Object endId) throws RhombusException {
		Long startCounter = isIdInRange(startId) ? getCounterAtId(startId) : counterRange.lowerEndpoint();
		Long endCounter = isIdInRange(endId) ? getCounterAtId(endId) : counterRange.upperEndpoint();
		return getIterator(ordering, Range.closed(startCounter, endCounter));
	}

	public Object getSuggestedIdForStartofNextIndex(TimebasedShardingStrategy shardingStrategyOfNextIndex) throws RhombusException {
		Object paddedId = getIdAtCounter(counterRange.upperEndpoint()+10L,shardingStrategy);
		Long spacingOfNextIndex = getSpacingForShardingStrategy(this.objectsPerShard, shardingStrategyOfNextIndex);

		if(idType.equals(CField.CDataType.TIMEUUID)){
			UUID offsetUUID = (UUID)paddedId;
			return makeTimeUUIDFromLowestTimeUnit(offsetUUID.timestamp() + (2*spacingOfNextIndex));
		}

		if(idType.equals(CField.CDataType.VARCHAR)){
			Long offsetLong = Long.valueOf(paddedId.toString());
			return (offsetLong + (2*spacingOfNextIndex))+"";
		}

		if(idType.equals(CField.CDataType.BIGINT)){
			Long offsetLong = (Long)paddedId;
			return offsetLong + (2*spacingOfNextIndex);
		}

		throw new RhombusException("Index Id type not compatible with faking it");
	}

	public Boolean isIdInRange(Object id) throws RhombusException{
		Long counter = getCounterAtId(id);
		if(counter < counterRange.lowerEndpoint()){
			return false;
		}

		if(counter > counterRange.upperEndpoint()){
			return false;
		}

		return true;
	}

	public static long getMillis(UUID uuid) {
		return uuid.timestamp() / 10000;
	}

	public Long getCounterAtId(Object id) throws RhombusException {
		if(idType.equals(CField.CDataType.TIMEUUID)){
			UUID idUUID = (UUID)id;
			Long idMillis = getMillis(idUUID);
			Long indexOfShard = shardingStrategy.getShardKey(idMillis);
			Long shardMillis = PARDOT_EPOCH + (indexOfShard*getTimeUnitsPerShard(shardingStrategy));
			Long itemInShard = (idMillis - shardMillis)/spacing;
			return indexOfShard+itemInShard;
		}

		if(idType.equals(CField.CDataType.VARCHAR)){
			return Long.valueOf(id.toString());
		}

		if(idType.equals(CField.CDataType.BIGINT)){
			return (Long)id;
		}

		throw new RhombusException("Index Id type not compatible with faking it");
	}

	public Object getIdAtCounter(Long counter, TimebasedShardingStrategy shardingStrategy)  throws RhombusException {
		//get current shard number
		Long indexOfShard = startingShardNumber + (counter/objectsPerShard);

		//get location in that shard
		Long itemInShard = counter%objectsPerShard;

		//get the id unit offset at the beginning of that shard

		//turn it into an id

		if(idType.equals(CField.CDataType.TIMEUUID)){
			Long timestamp = PARDOT_EPOCH + (indexOfShard*getTimeUnitsPerShard(shardingStrategy));
			if(shardingStrategy.getClass().equals(ShardingStrategyMonthly.class)){
				//months are weird so we need to special case
				int years = (int)(indexOfShard/12);
				int months = (int)((indexOfShard%12))+1;
				DateTime dt = new DateTime(years+2000, months, 1, 0, 0 ,DateTimeZone.UTC);
				timestamp = dt.getMillis();
			}
			timestamp+=(spacing*itemInShard);
			return UUIDs.startOf(timestamp);
		}

		if(idType.equals(CField.CDataType.VARCHAR)){
			return counter+"";
		}

		if(idType.equals(CField.CDataType.BIGINT)){
			return Long.valueOf(counter);
		}

		throw new RhombusException("Index Id type not compatible with faking it");

	}

	public static UUID makeTimeUUIDFromLowestTimeUnit(Long timestamp){
		return UUIDs.startOf(timestamp);
//		// UUID v1 timestamp must be in 100-nanoseconds interval since 00:00:00.000 15 Oct 1582.
//		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"));
//		c.set(Calendar.YEAR, 1582);
//		c.set(Calendar.MONTH, Calendar.OCTOBER);
//		c.set(Calendar.DAY_OF_MONTH, 15);
//		c.set(Calendar.HOUR_OF_DAY, 0);
//		c.set(Calendar.MINUTE, 0);
//		c.set(Calendar.SECOND, 0);
//		c.set(Calendar.MILLISECOND, 0);
//		timestamp = timestamp - c.getTimeInMillis()*10000;
//
//		UUIDs.startOf(timestamp);
//		long clock = 42;
//		long node = 0x0000010000000000L;
//
//		long CLOCK_SEQ_AND_NODE = 0;
//		CLOCK_SEQ_AND_NODE |= (clock & 0x0000000000003FFFL) << 48;
//		CLOCK_SEQ_AND_NODE |= 0x8000000000000000L;
//		CLOCK_SEQ_AND_NODE |= node;
//
//		long msb = 0L;
//		msb |= (0x00000000ffffffffL & timestamp) << 32;
//		msb |= (0x0000ffff00000000L & timestamp) >>> 16;
//		msb |= (0x0fff000000000000L & timestamp) >>> 48;
//		msb |= 0x0000000000001000L; // sets the version to 1.
//		return new UUID(msb, CLOCK_SEQ_AND_NODE);
	}

	public Long getSpacingForShardingStrategy(Long objectsPerShard)
	{
		return getSpacingForShardingStrategy(objectsPerShard, this.shardingStrategy);
	}


	public Long getTimeUnitsPerShard(TimebasedShardingStrategy shardingStrategy){
		if(shardingStrategy.getClass().equals(ShardingStrategyNone.class)){
			return 1L;
		}

		Long timeUnitsPerSecond = 1000L; //milliseconds

		Long timeUnitsPerShard = 1L;
		if(shardingStrategy.getClass().equals(ShardingStrategyHourly.class)){
			timeUnitsPerShard = timeUnitsPerSecond * // 1 second
					(60L) * // 1 minute
					(60L); // 1 hour
		}

		if(shardingStrategy.getClass().equals(ShardingStrategyDaily.class)){
			timeUnitsPerShard = timeUnitsPerSecond * // 1 second
					(60L) *  // 1 minute
					(60L) * // 1 hour
					(24L); // 1 Day
		}

		if(shardingStrategy.getClass().equals(ShardingStrategyWeekly.class)){
			timeUnitsPerShard = timeUnitsPerSecond * // 1 second
					(60L) *   // 1 minute
					(60L) *  // 1 hour
					(24L) * // 1 Day
					(7L);  // 1 Week
		}

		if(shardingStrategy.getClass().equals(ShardingStrategyMonthly.class)){
			timeUnitsPerShard = timeUnitsPerSecond * // 1 second
					(60L) *   // 1 minute
					(60L) *  // 1 hour
					(24L) * // 1 Day
					(28L); // 1 Monthish
		}

		return timeUnitsPerShard;
	}

	public Long getSpacingForShardingStrategy(Long objectsPerShard, TimebasedShardingStrategy shardingStrategy)
	{
		return getTimeUnitsPerShard(shardingStrategy)/objectsPerShard;
	}

	public class IdInRange {
		private Object id;
		private Long counterValue;

		public IdInRange(Object id, Long counterValue){
			this.id = id;
			this.counterValue = counterValue;
		}

		public Object getId() {
			return id;
		}

		public void setId(Object id) {
			this.id = id;
		}

		public Long getCounterValue() {
			return counterValue;
		}

		public void setCounterValue(Long counterValue) {
			this.counterValue = counterValue;
		}
	}

	public class IdIterator implements Iterator<IdInRange> {

		private Iterator<Long> rangeIterator;

		public IdIterator(Iterator<Long> rangeIterator){
			this.rangeIterator = rangeIterator;
		}

		public boolean hasNext(){
			return rangeIterator.hasNext();
		}

		public IdInRange next() {
			try{
				Long counter = rangeIterator.next();
				return new IdInRange(getIdAtCounter(counter,shardingStrategy),counter);
			}
			catch (RhombusException re){
				//this should never happen
				//TODO: add proper logging here
				re.printStackTrace();
				return null;
			}
		}

		public void remove(){
			rangeIterator.remove();
		}
	}

}
