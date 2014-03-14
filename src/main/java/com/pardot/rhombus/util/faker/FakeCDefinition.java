package com.pardot.rhombus.util.faker;

import com.pardot.rhombus.cobject.CDefinition;

/**
 * User: Rob Righter
 * Date: 3/13/14
 */
public class FakeCDefinition {

	private CDefinition cdef;
	private Long minimumUniqueIndexHits;
	private Long countPerIndexShard;

	public FakeCDefinition(CDefinition def, Long minimumUniqueIndexHits, Long countPerIndexShard)
	{
		this.cdef = def;
		this.minimumUniqueIndexHits = minimumUniqueIndexHits;
		this.countPerIndexShard = countPerIndexShard;
	}


}
