package com.pardot.rhombus.util.faker;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.pardot.rhombus.ObjectMapper;
import com.pardot.rhombus.RhombusException;
import com.pardot.rhombus.cobject.CField;
import com.pardot.rhombus.cobject.CObjectOrdering;
import com.pardot.rhombus.cobject.shardingstrategy.*;
import org.omg.CosNaming._NamingContextExtStub;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * User: Rob Righter
 * Date: 3/13/14
 */
public class FakeIdRange {

	private Range<Long> counterRange;
	private long spacing;
	private TimebasedShardingStrategy shardingStrategy;
	private CField.CDataType idType;
	private Object offset;

	public FakeIdRange(CField.CDataType idType, Object startId, Long totalObjects, Long objectsPerShard, TimebasedShardingStrategy shardingStrategy){
		this.shardingStrategy = shardingStrategy;
		this.spacing = getSpacingForShardingStrategy(objectsPerShard);
		this.idType = idType;
		this.offset = startId;
		this.counterRange = Range.open(1L,totalObjects);
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
		return getIterator(ordering, Range.open(startCounter,endCounter));
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

	private Long getCounterAtId(Object id) throws RhombusException {
		if(idType.equals(CField.CDataType.TIMEUUID)){
			UUID offsetUUID = (UUID)offset;
			UUID idUUID = (UUID)id;
			return (idUUID.timestamp() - offsetUUID.timestamp())/spacing;
		}

		if(idType.equals(CField.CDataType.VARCHAR)){
			return Long.valueOf(id.toString().substring(offset.toString().length()))/spacing;
		}

		if(idType.equals(CField.CDataType.BIGINT)){
			Long idLong = (Long)id;
			Long offsetLong = (Long)offset;
			return (idLong - offsetLong)/spacing;
		}

		throw new RhombusException("Index Id type not compatible with faking it");
	}

	public Object getIdAtCounter(Long counter)  throws RhombusException {
		if(idType.equals(CField.CDataType.TIMEUUID)){
			UUID offsetUUID = (UUID)offset;
			return makeTimeUUIDFromNanos(offsetUUID.timestamp() + (counter * spacing));
		}

		if(idType.equals(CField.CDataType.VARCHAR)){
			return offset.toString()+(counter*spacing)+"";
		}

		if(idType.equals(CField.CDataType.BIGINT)){
			return ((Long)offset)+(counter*spacing*1L);
		}

		throw new RhombusException("Index Id type not compatible with faking it");

	}

	public UUID makeTimeUUIDFromNanos(Long nanos){
		return null;
	}

	public Long getSpacingForShardingStrategy(Long objectsPerShard)
	{
		if(shardingStrategy.equals(ShardingStrategyNone.class)){
			return 1L;
		}

		Long nanosecondsPerShard = 1L;
		if(shardingStrategy.equals(ShardingStrategyHourly.class)){
			nanosecondsPerShard = (1000000L) * (1000L) * // 1 second
					(60L) * // 1 minute
					(60L); // 1 hour
		}

		if(shardingStrategy.equals(ShardingStrategyDaily.class)){
			nanosecondsPerShard = (1000000L) * (1000L) * // 1 second
					(60L) *  // 1 minute
					(60L) * // 1 hour
					(24L); // 1 Day
		}

		if(shardingStrategy.equals(ShardingStrategyWeekly.class)){
			nanosecondsPerShard = (1000000L) * (1000L) * // 1 second
					(60L) *   // 1 minute
					(60L) *  // 1 hour
					(24L) * // 1 Day
					(7L);  // 1 Week
		}

		if(shardingStrategy.equals(ShardingStrategyMonthly.class)){
			nanosecondsPerShard = (1000000L) * (1000L) * // 1 second
					(60L) *   // 1 minute
					(60L) *  // 1 hour
					(24L) * // 1 Day
					(28L); // 1 Monthish
		}

		return nanosecondsPerShard/objectsPerShard;
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
				return new IdInRange(getIdAtCounter(counter),counter);
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
