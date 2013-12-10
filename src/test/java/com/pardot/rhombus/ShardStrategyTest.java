package com.pardot.rhombus;

import com.google.common.collect.Range;
import com.pardot.rhombus.cobject.shardingstrategy.*;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Pardot, An ExactTarget Company
 * User: robrighter
 * Date: 4/16/13
 */
public class ShardStrategyTest extends TestCase {
	/**
	 * Create the test case
	 *
	 * @param testName name of the test case
	 */
	public ShardStrategyTest(String testName) {
		super( testName );
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite() {
		return new TestSuite( ShardStrategyTest.class );
	}

	public void testShardingStrategyHourly() throws ShardStrategyException {
		ShardingStrategyHourly subject = new ShardingStrategyHourly();
		DateTime d = new DateTime(2000,1,1,0,0,0, DateTimeZone.UTC);
		long actual = subject.getShardKey(d.getMillis());
		// 0 based, but shard ids are relative so it doesn't really matter
		assertEquals("Should generate correct shard key for date", 0, actual);

		d = new DateTime(2006,1,1,1,0,0, DateTimeZone.UTC);
		actual = subject.getShardKey(d.getMillis());
		assertEquals("Should generate correct shard key for date given offset", 52609, actual);

		//test with offset
		subject = new ShardingStrategyHourly();
		subject.setOffset(20);

		d = new DateTime(2000,1,10,0,0,0, DateTimeZone.UTC);
		actual = subject.getShardKey(d.getMillis());
		// 0 based, doesn't really matter
		assertEquals("Should generate correct shard key for date", 236, actual);

		//test the range bounded
		subject = new ShardingStrategyHourly();
		DateTime d1 = new DateTime(2013,2,22,2,0,0, DateTimeZone.UTC); //115226
		DateTime d2 = new DateTime(2014,2,22,2,0,0, DateTimeZone.UTC); //123986
		Range<Long> range = subject.getShardKeyRange(d1.getMillis(),d2.getMillis());
		assertEquals("Range should have appropriate start point", 115226L, range.lowerEndpoint().longValue());
		assertEquals("Range should have appropriate end point", 123986L, range.upperEndpoint().longValue());

		//test range auto bounded
		subject = new ShardingStrategyHourly();
		d1 = new DateTime(2011,2,22,2,0,0, DateTimeZone.UTC); //97682
		range = subject.getShardKeyRange(d1.getMillis(),null);
		assertEquals("Range should have appropriate start point", 97682L, range.lowerEndpoint().longValue());
		assertTrue("Range should have an upper bound", range.hasUpperBound());
		assertTrue("Range should have a lower bound",range.hasLowerBound());
	}

    public void testShardingStrategyDaily() throws ShardStrategyException {
        ShardingStrategyDaily subject = new ShardingStrategyDaily();
        DateTime d = new DateTime(2000,1,1,0,0,0, DateTimeZone.UTC);
        long actual = subject.getShardKey(d.getMillis());
        // 0 based, but shard ids are relative so it doesn't really matter
        assertEquals("Should generate correct shard key for date", 0, actual);

        d = new DateTime(2006,1,1,1,0,0, DateTimeZone.UTC);
        actual = subject.getShardKey(d.getMillis());
        assertEquals("Should generate correct shard key for date given offset", 2192, actual);

        //test with offset
        subject = new ShardingStrategyDaily();
        subject.setOffset(20);

        d = new DateTime(2000,1,10,0,0,0, DateTimeZone.UTC);
        actual = subject.getShardKey(d.getMillis());
        // 0 based, doesn't really matter
        assertEquals("Should generate correct shard key for date", 29, actual);

        //test the range bounded
        subject = new ShardingStrategyDaily();
        DateTime d1 = new DateTime(2013,2,22,2,0,0, DateTimeZone.UTC); //158
        DateTime d2 = new DateTime(2014,2,22,2,0,0, DateTimeZone.UTC); //170
        Range<Long> range = subject.getShardKeyRange(d1.getMillis(),d2.getMillis());
        assertEquals("Range should have appropriate start point", 4801L, range.lowerEndpoint().longValue());
        assertEquals("Range should have appropriate end point", 5166L, range.upperEndpoint().longValue());

        //test range auto bounded
        subject = new ShardingStrategyDaily();
        d1 = new DateTime(2011,2,22,2,0,0, DateTimeZone.UTC); //170
        range = subject.getShardKeyRange(d1.getMillis(),null);
        assertEquals("Range should have appropriate start point", 4070L, range.lowerEndpoint().longValue());
        assertTrue("Range should have an upper bound", range.hasUpperBound());
        assertTrue("Range should have a lower bound",range.hasLowerBound());
    }

    public void testShardingStrategyWeekly() throws ShardStrategyException {
        ShardingStrategyWeekly subject = new ShardingStrategyWeekly();
        DateTime d = new DateTime(2000,1,1,0,0,0, DateTimeZone.UTC);
        long actual = subject.getShardKey(d.getMillis());
        // 0 based, but shard ids are relative so it doesn't really matter
        assertEquals("Should generate correct shard key for date", 0, actual);

        // In the ISO week system, a year has 53 weeks every 5.6338 years, so we need to check between 5 and 6 years to make sure
        // we handle the "leap week" thing correctly, even though we're not using weekyears
        d = new DateTime(2005,12,31,1,0,0, DateTimeZone.UTC);
        actual = subject.getShardKey(d.getMillis());
        assertEquals("Should generate correct shard key for date given offset", 313,actual);

        d = new DateTime(2006,1,1,1,0,0, DateTimeZone.UTC);
        actual = subject.getShardKey(d.getMillis());
        assertEquals("Should generate correct shard key for date given offset", 313,actual);

        //test with offset
        subject = new ShardingStrategyWeekly();
        subject.setOffset(20);

        d = new DateTime(2000,10,1,0,0,0, DateTimeZone.UTC);
        actual = subject.getShardKey(d.getMillis());
        // 0 based, doesn't really matter
        assertEquals("Should generate correct shard key for date", 59, actual);

        //test the range bounded
        subject = new ShardingStrategyWeekly();
        DateTime d1 = new DateTime(2013,2,22,2,0,0, DateTimeZone.UTC); //158
        DateTime d2 = new DateTime(2014,2,22,2,0,0, DateTimeZone.UTC); //170
        Range<Long> range = subject.getShardKeyRange(d1.getMillis(),d2.getMillis());
        assertEquals("Range should have appropriate start point", 685L, range.lowerEndpoint().longValue());
        assertEquals("Range should have appropriate end point", 738L, range.upperEndpoint().longValue());

        //test range auto bounded
        subject = new ShardingStrategyWeekly();
        d1 = new DateTime(2011,2,22,2,0,0, DateTimeZone.UTC); //170
        range = subject.getShardKeyRange(d1.getMillis(),null);
        assertEquals("Range should have appropriate start point", 581L, range.lowerEndpoint().longValue());
        assertTrue("Range should have an upper bound", range.hasUpperBound());
        assertTrue("Range should have a lower bound",range.hasLowerBound());
    }

	public void testShardingStrategyMonthly() throws ShardStrategyException {
		ShardingStrategyMonthly subject = new ShardingStrategyMonthly();
		DateTime d = new DateTime(2013,2,22,1,0,0, DateTimeZone.UTC);
		long actual = subject.getShardKey(d.getMillis());
		assertEquals("Should generate correct shard key for date", 158,actual);

		//test with offset
		subject = new ShardingStrategyMonthly();
		subject.setOffset(20);
		d = new DateTime(2013,2,22,1,0,0, DateTimeZone.UTC);
		actual = subject.getShardKey(d.getMillis());
		assertEquals("Should generate correct shard key for date given offset", 178,actual);

		//test the range bounded
		subject = new ShardingStrategyMonthly();
		DateTime d1 = new DateTime(2013,2,22,2,0,0, DateTimeZone.UTC); //158
		DateTime d2 = new DateTime(2014,2,22,2,0,0, DateTimeZone.UTC); //170
		Range<Long> range = subject.getShardKeyRange(d1.getMillis(),d2.getMillis());
		assertEquals("Range should have appropriate start point", 158L, range.lowerEndpoint().longValue());
		assertEquals("Range should have appropriate end point", 170L, range.upperEndpoint().longValue());

		//test range auto bounded
		subject = new ShardingStrategyMonthly();
		d1 = new DateTime(2011,2,22,2,0,0, DateTimeZone.UTC); //170
		range = subject.getShardKeyRange(Long.valueOf(d1.getMillis()),null);
		assertEquals("Range should have appropriate start point", 134L, range.lowerEndpoint().longValue());
		assertTrue("Range should have an upper bound", range.hasUpperBound());
		assertTrue("Range should have a lower bound",range.hasLowerBound());
	}

	public void testShardingStrategyNone() throws ShardStrategyException {
		ShardingStrategyNone subject = new ShardingStrategyNone();
		DateTime d = new DateTime(2013,2,22,1,0,0, DateTimeZone.UTC);
		long actual = subject.getShardKey(d.getMillis());
		assertEquals("Should generate correct shard key for date", 1L ,actual);

		//test with offset
		subject = new ShardingStrategyNone();
		subject.setOffset(20);
		d = new DateTime(2013,2,22,1,0,0, DateTimeZone.UTC);
		actual = subject.getShardKey(d.getMillis());
		assertEquals("Should generate correct shard key for date given offset", 21L ,actual);

		//test the range bounded
		subject = new ShardingStrategyNone();
		DateTime d1 = new DateTime(2013,2,22,2,0,0, DateTimeZone.UTC);
		DateTime d2 = new DateTime(2014,2,22,2,0,0, DateTimeZone.UTC);
		Range<Long> range = subject.getShardKeyRange(d1.getMillis(),d2.getMillis());
		assertEquals("Range should have appropriate start point", 1L , range.lowerEndpoint().longValue());
		assertEquals("Range should have appropriate end point", 1L , range.upperEndpoint().longValue());

		//test range unbounded
		subject = new ShardingStrategyNone();
		d1 = new DateTime(2012,2,22,2,0,0, DateTimeZone.UTC);
		range = subject.getShardKeyRange(Long.valueOf(d1.getMillis()),null);
		assertEquals("Range should have appropriate start point", 1L , range.lowerEndpoint().longValue());
		assertTrue("Range should be just 1",range.upperEndpoint().longValue() == 1L);
		assertTrue("Range should be just 1",range.lowerEndpoint().longValue() == range.upperEndpoint().longValue());
	}
}
