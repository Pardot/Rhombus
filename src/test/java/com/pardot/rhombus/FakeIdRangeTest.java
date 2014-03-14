package com.pardot.rhombus;

import com.datastax.driver.core.utils.UUIDs;
import com.pardot.rhombus.cobject.CField;
import com.pardot.rhombus.cobject.CObjectOrdering;
import com.pardot.rhombus.cobject.shardingstrategy.ShardingStrategyMonthly;
import com.pardot.rhombus.cobject.shardingstrategy.TimebasedShardingStrategy;
import com.pardot.rhombus.util.UuidUtil;
import com.pardot.rhombus.util.faker.FakeIdRange;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Date;
import java.util.Iterator;
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
	public void getIteratorTestMonthly() throws RhombusException {
		Long millistamp = 946740000000L;
		millistamp = System.currentTimeMillis();
		System.out.println(millistamp);
		UUID startingUUID = UUIDs.startOf(millistamp);
		System.out.println("UUID of start is "+startingUUID+" (DATE= "+UuidUtil.getDateFromUUID(startingUUID)+" )");


		TimebasedShardingStrategy shardingStrategy = new ShardingStrategyMonthly();
		int numberPerShard = 16;
		long totalNumberOfObjects = 10000L;
		FakeIdRange subject = new FakeIdRange(CField.CDataType.TIMEUUID,startingUUID,totalNumberOfObjects,(long)numberPerShard, shardingStrategy);
		Iterator<FakeIdRange.IdInRange> it = subject.getIterator(CObjectOrdering.ASCENDING);

		long counter = 0;
		int lastmonth = 0;
		int countSinceChange = numberPerShard;
		while(it.hasNext()){
			counter++;
			FakeIdRange.IdInRange id = it.next();
			Date dt = UuidUtil.getDateFromUUID((UUID)id.getId());
			if(dt.getMonth() == lastmonth){
				assert(countSinceChange < numberPerShard);
				countSinceChange++;
			}
			else{
				assertEquals(numberPerShard,countSinceChange);
				countSinceChange=1;
			};
			lastmonth = dt.getMonth();
			System.out.println("Expected Counter="+counter);
			System.out.println("Id Counter="+id.getCounterValue());
			System.out.println("Lookup Counter="+subject.getCounterAtId(id.getId()));
			//assertEquals(subject.getCounterAtId(id.getId()), id.getCounterValue());
			System.out.println(counter + ": " + UuidUtil.getDateFromUUID((UUID)id.getId()).toString());
		}

		assertEquals(totalNumberOfObjects,counter);



	}

}
