package com.pardot.rhombus.cobject;

import java.util.Map;

/**
 * User: Rob Righter
 * Date: 10/31/13
 */
public interface CObjectVisitor {

	public void visit(Map<String,Object> object);

	public boolean shouldInclude(Map<String,Object> object);
}
