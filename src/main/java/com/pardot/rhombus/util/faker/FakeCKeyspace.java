package com.pardot.rhombus.util.faker;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pardot.rhombus.RhombusException;
import com.pardot.rhombus.cobject.CDefinition;
import com.pardot.rhombus.cobject.CIndex;
import com.pardot.rhombus.cobject.CKeyspaceDefinition;

import java.util.List;
import java.util.Map;

/**
 * User: Rob Righter
 * Date: 3/13/14
 */
public class FakeCKeyspace {

	private CKeyspaceDefinition kdef;
	private Long totalWideRowsPerIndex;
	private Long totalObjectsPerWideRange;
	private Long objectsPerShard;

	private Map<String,FakeCDefinition> fakeCDefs;

	public FakeCKeyspace(CKeyspaceDefinition def, Long totalWideRowsPerIndex,
						   Long totalObjectsPerWideRange,
						   Long objectsPerShard) throws RhombusException
	{
		this.kdef = def;
		this.totalWideRowsPerIndex = totalWideRowsPerIndex;
		this.totalObjectsPerWideRange = totalObjectsPerWideRange;
		this.objectsPerShard = objectsPerShard;
		this.buildFakeCDefs();
	}

	public Map<String,FakeCDefinition> buildFakeCDefs() throws RhombusException {
		this.fakeCDefs = Maps.newHashMap();
		for(String cdefName: this.kdef.getDefinitions().keySet()){
			FakeCDefinition toadd = new FakeCDefinition(kdef.getDefinitions().get(cdefName),totalWideRowsPerIndex,totalObjectsPerWideRange,objectsPerShard);
			this.fakeCDefs.put(cdefName,toadd);

		}
		return this.fakeCDefs;
	}

	public Map<String,FakeCDefinition> getFakeCDefs() {
		return fakeCDefs;
	}
}
