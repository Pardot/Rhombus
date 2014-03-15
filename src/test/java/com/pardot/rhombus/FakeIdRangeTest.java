package com.pardot.rhombus;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Maps;
import com.pardot.rhombus.cobject.CField;
import com.pardot.rhombus.cobject.CObjectOrdering;
import com.pardot.rhombus.cobject.shardingstrategy.*;
import com.pardot.rhombus.util.UuidUtil;
import com.pardot.rhombus.util.faker.FakeIdRange;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * User: Rob Righter
 * Date: 3/14/14
 */
public class FakeIdRangeTest {

//	@Test
//	public void  makeTimeUUIDFromTimeUnitTest(){
//
//		long expectedTimestamp = System.currentTimeMillis();
//		System.out.println(expectedTimestamp);
//		UUID actual = FakeIdRange.makeTimeUUIDFromLowestTimeUnit(expectedTimestamp);
//		System.out.println(actual.toString());
//		UUIDs.
//		assertEquals(expectedTimestamp*100000L, actual.timestamp());
//
//		//assert that its always the same
////		Long timestamp = 13948172300040000L;
////		UUID uuid = FakeIdRange.makeTimeUUIDFromLowestTimeUnit(timestamp);
////		assertEquals("ef35b740-8dc9-1031-802a-010000000000",uuid.toString());
//
//		//assert that the date converts to something sane
//		assertEquals("SOMETHING SANE",UuidUtil.getDateFromUUID(actual).toString());;
//	}

	@Test
	public void getMillisAtShardKeyMonthlyTest() throws RhombusException{
		Long millistamp = System.currentTimeMillis();
		UUID startingUUID = UUIDs.startOf(millistamp);
		//System.out.println("UUID of start is "+startingUUID+" (DATE= "+UuidUtil.getDateFromUUID(startingUUID)+" )");
		TimebasedShardingStrategy shardingStrategy = new ShardingStrategyMonthly();
		FakeIdRange subject = new FakeIdRange(CField.CDataType.TIMEUUID,startingUUID,95L,10L, shardingStrategy, "testing");
		long counter = 90L;
		UUID id = (UUID)subject.getIdAtCounter(counter,shardingStrategy);
		//System.out.println("UUID of the id is "+id+" (DATE= "+UuidUtil.getDateFromUUID(id)+" ) Millis:"+UuidUtil.convertUUIDToJavaMillis(id));
		long actual = subject.getCounterAtId(id);
		assertEquals(counter,actual);
	}

	@Test
	public void getMillisAtShardKeyWeeklyTest() throws RhombusException{
		Long millistamp = System.currentTimeMillis();
		UUID startingUUID = UUIDs.startOf(millistamp);
		//System.out.println("UUID of start is "+startingUUID+" (DATE= "+UuidUtil.getDateFromUUID(startingUUID)+" )");
		TimebasedShardingStrategy shardingStrategy = new ShardingStrategyWeekly();
		FakeIdRange subject = new FakeIdRange(CField.CDataType.TIMEUUID,startingUUID,950L,3L, shardingStrategy, "testing");
		long counter = 8L;
		UUID id = (UUID)subject.getIdAtCounter(counter,shardingStrategy);
		//System.out.println("UUID of the id is "+id+" (DATE= "+UuidUtil.getDateFromUUID(id)+" ) Millis:"+UuidUtil.convertUUIDToJavaMillis(id));
		long actual = subject.getCounterAtId(id);
		assertEquals(counter,actual);
	}

	@Test
	public void getMillisAtShardKeyDailyTest() throws RhombusException{
		Long millistamp = System.currentTimeMillis();
		UUID startingUUID = UUIDs.startOf(millistamp);
		//System.out.println("UUID of start is "+startingUUID+" (DATE= "+UuidUtil.getDateFromUUID(startingUUID)+" )");
		TimebasedShardingStrategy shardingStrategy = new ShardingStrategyDaily();
		FakeIdRange subject = new FakeIdRange(CField.CDataType.TIMEUUID,startingUUID,950L,3L, shardingStrategy, "testing");
		long counter = 8L;
		UUID id = (UUID)subject.getIdAtCounter(counter,shardingStrategy);
		//System.out.println("UUID of the id is "+id+" (DATE= "+UuidUtil.getDateFromUUID(id)+" ) Millis:"+UuidUtil.convertUUIDToJavaMillis(id));
		long actual = subject.getCounterAtId(id);
		assertEquals(counter,actual);
	}

	@Test
	public void getMillisAtShardKeyNoneTest() throws RhombusException{
		Long millistamp = System.currentTimeMillis();
		UUID startingUUID = UUIDs.startOf(millistamp);
		//System.out.println("UUID of start is "+startingUUID+" (DATE= "+UuidUtil.getDateFromUUID(startingUUID)+" )");
		TimebasedShardingStrategy shardingStrategy = new ShardingStrategyNone();
		FakeIdRange subject = new FakeIdRange(CField.CDataType.TIMEUUID,startingUUID,950L,3L, shardingStrategy, "testing");
		long counter = 8L;
		UUID id = (UUID)subject.getIdAtCounter(counter,shardingStrategy);
		//System.out.println("UUID of the id is "+id+" (DATE= "+UuidUtil.getDateFromUUID(id)+" ) Millis:"+UuidUtil.convertUUIDToJavaMillis(id));
		long actual = subject.getCounterAtId(id);
		assertEquals(counter,actual);
	}

	@Test
	public void getIteratorTestMonthly() throws RhombusException {
		Long millistamp = 946740000000L;
		millistamp = System.currentTimeMillis();
		System.out.println(millistamp);
		UUID startingUUID = UUIDs.startOf(millistamp);
		System.out.println("UUID of start is "+startingUUID+" (DATE= "+UuidUtil.getDateFromUUID(startingUUID)+" )");


		TimebasedShardingStrategy shardingStrategy = new ShardingStrategyMonthly();
		int numberPerShard = 3;
		long totalNumberOfObjects = 950L;
		FakeIdRange subject = new FakeIdRange(CField.CDataType.TIMEUUID,startingUUID,totalNumberOfObjects,(long)numberPerShard, shardingStrategy, "testing");
		Iterator<FakeIdRange.IdInRange> it = subject.getIterator(CObjectOrdering.ASCENDING);

		long counter = 0;
		int lastmonth = -1;
		int countSinceChange = 0;
		while(it.hasNext()){
			counter++;
			FakeIdRange.IdInRange id = it.next();
			DateTime dt = UuidUtil.getDateFromUUID((UUID)id.getId());
			if(lastmonth == -1){
				//initialization case
				countSinceChange = 1;
				lastmonth = dt.getMonthOfYear();
			}
			if(dt.getMonthOfYear() == lastmonth){
				assert(countSinceChange < numberPerShard);
				countSinceChange++;
			}
			else{
				assertEquals(numberPerShard,countSinceChange);
				countSinceChange=1;
			};
			lastmonth = dt.getMonthOfYear();
			assertEquals(subject.getCounterAtId(id.getId()), id.getCounterValue());
		}
		assertEquals(totalNumberOfObjects,counter);
	}

	@Test
	public void getIteratorTestWeekly() throws RhombusException {
		Long millistamp = 946740000000L;
		millistamp = System.currentTimeMillis();
		System.out.println(millistamp);
		UUID startingUUID = UUIDs.startOf(millistamp);
		System.out.println("UUID of start is "+startingUUID+" (DATE= "+UuidUtil.getDateFromUUID(startingUUID)+" )");


		TimebasedShardingStrategy shardingStrategy = new ShardingStrategyWeekly();
		int numberPerShard = 3;
		long totalNumberOfObjects = 950L;
		FakeIdRange subject = new FakeIdRange(CField.CDataType.TIMEUUID,startingUUID,totalNumberOfObjects,(long)numberPerShard, shardingStrategy, "testing");
		Iterator<FakeIdRange.IdInRange> it = subject.getIterator(CObjectOrdering.ASCENDING);

		long counter = 0;
		int lastweek = -1;
		int countSinceChange = 0;
		while(it.hasNext()){
			counter++;
			FakeIdRange.IdInRange id = it.next();
			DateTime dt = UuidUtil.getDateFromUUID((UUID)id.getId());
			if(lastweek == -1){
				//initialization case
				countSinceChange = 1;
				lastweek = dt.getWeekOfWeekyear();
			}
			else if(dt.getWeekOfWeekyear() == lastweek){
				assert(countSinceChange < numberPerShard);
				countSinceChange++;
			}
			else{
				assertEquals(numberPerShard,countSinceChange);
				countSinceChange=1;
			};
			lastweek = dt.getWeekOfWeekyear();
			//System.out.println("countSinceChange = " + countSinceChange);
			//System.out.println("Actual Counter = " + counter);
			//System.out.println("Id Counter = " + id.getCounterValue());
			//System.out.println("Counter at ID = " + subject.getCounterAtId(id.getId()));
			//System.out.println("Date on UUID is: " + UuidUtil.getDateFromUUID((UUID)id.getId()));
			assertEquals(subject.getCounterAtId(id.getId()), id.getCounterValue());
		}
		assertEquals(totalNumberOfObjects,counter);
	}

	@Test
	public void getIteratorDailyWeekly() throws RhombusException {
		Long millistamp = System.currentTimeMillis();
		System.out.println(millistamp);
		UUID startingUUID = UUIDs.startOf(millistamp);
		System.out.println("UUID of start is "+startingUUID+" (DATE= "+UuidUtil.getDateFromUUID(startingUUID)+" )");

		TimebasedShardingStrategy shardingStrategy = new ShardingStrategyDaily();
		int numberPerShard = 3;
		long totalNumberOfObjects = 950L;
		FakeIdRange subject = new FakeIdRange(CField.CDataType.TIMEUUID,startingUUID,totalNumberOfObjects,(long)numberPerShard, shardingStrategy, "testing");
		Iterator<FakeIdRange.IdInRange> it = subject.getIterator(CObjectOrdering.ASCENDING);

		int counter = 0;
		Map<String,Long> shards = Maps.newTreeMap();
		while(it.hasNext()){
			counter++;
			FakeIdRange.IdInRange id = it.next();
			DateTime dt = UuidUtil.getDateFromUUID((UUID)id.getId());
			if(shards.get(dt.getDayOfYear()+"-"+dt.getYear()) == null){
				shards.put(dt.getDayOfYear() + "-" + dt.getYear(), 1L);
			}
			else{
				shards.put(dt.getDayOfYear()+"-"+dt.getYear(), shards.get(dt.getDayOfYear()+"-"+dt.getYear())+1L);
			}
			assertEquals(subject.getCounterAtId(id.getId()), id.getCounterValue());
		}
		assertEquals(totalNumberOfObjects, counter);
		
		for(Long shardcount : shards.values()){
			assert(Long.valueOf(numberPerShard) >= Long.valueOf(shardcount));
		}
	}

}
