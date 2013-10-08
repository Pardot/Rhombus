package com.pardot.rhombus;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Objects;
import com.pardot.rhombus.cobject.CObjectOrdering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.SortedMap;
import java.util.UUID;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 4/17/13
 */
public class Criteria {
	private static final Logger logger = LoggerFactory.getLogger(Criteria.class);

	private SortedMap<String, Object> indexKeys;
	private CObjectOrdering ordering;
	private UUID startUuid;
	private UUID endUuid;
	private Long limit = 0L;
	private Boolean inclusive = true;


	public String toString() {
		return Objects.toStringHelper(this.getClass())
				.add("indexKeys", indexKeys)
				.add("ordering", ordering)
				.add("startTimestamp", uuidToDateString(startUuid))
				.add("endTimestamp", uuidToDateString(endUuid))
				.add("limit", limit)
				.add("inclusive", inclusive)
				.toString();
	}

	private String uuidToDateString(UUID uuid) {
		if(uuid == null) {
			return null;
		} else {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ssZ");
			Date date = new Date(UUIDs.unixTimestamp(uuid));
			return sdf.format(date);
		}
	}
	public SortedMap<String, Object> getIndexKeys() {
		return indexKeys;
	}

	public void setIndexKeys(SortedMap<String, Object> indexKeys) {
		this.indexKeys = indexKeys;
	}

	public CObjectOrdering getOrdering() {
		return ordering;
	}

	public void setOrdering(String ordering) {
		this.ordering = CObjectOrdering.fromString(ordering);
	}

	public UUID getStartUuid() {
		return startUuid;
	}

	public void setStartUuid(UUID startUuid) {
		this.startUuid = startUuid;
	}

	public UUID getEndUuid() {
		return endUuid;
	}

	public void setEndUuid(UUID endUuid) {
		this.endUuid = endUuid;
	}

	public void setStartTimestamp(Long startTimestamp) {
		this.startUuid = UUIDs.startOf(startTimestamp);
	}

	public void setEndTimestamp(Long endTimestamp) {
		this.endUuid = UUIDs.endOf(endTimestamp);
	}

	public Long getLimit() {
		return limit;
	}

	public void setLimit(Long limit) {
		this.limit = limit;
	}

	public Boolean getInclusive() {
		return inclusive;
	}

	public void setInclusive(Boolean inclusive) {
		this.inclusive = inclusive;
	}
}
