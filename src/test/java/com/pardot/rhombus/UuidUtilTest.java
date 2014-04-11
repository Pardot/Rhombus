package com.pardot.rhombus;

import com.pardot.rhombus.util.UuidUtil;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.UUID;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 5/6/13
 */
public class UuidUtilTest {

	@Test
	public void testUuidUtil() {
		int namespace = 47;
		long name = 722338;
		UUID namespaceUuid = UuidUtil.namespaceUUID(namespace, name);

        assertEquals(namespace, UuidUtil.namespaceFromUUID(namespaceUuid).intValue());
        assertEquals(name, UuidUtil.nameFromUUID(namespaceUuid).longValue());
	}

    @Test
    public void testMaxValues() {
        int namespace = 0xffffffff;
        long name = 0xffffffffffffffffL;
        UUID namespaceUuid = UuidUtil.namespaceUUID(namespace, name);

        assertEquals(namespace, UuidUtil.namespaceFromUUID(namespaceUuid).intValue());
        assertEquals(name, UuidUtil.nameFromUUID(namespaceUuid).longValue());
    }

    @Test
    public void testMinValues() {
        int namespace = 1;
        long name = 1;
        UUID namespaceUuid = UuidUtil.namespaceUUID(namespace, name);

        assertEquals(namespace, UuidUtil.namespaceFromUUID(namespaceUuid).intValue());
        assertEquals(name, UuidUtil.nameFromUUID(namespaceUuid).longValue());
    }

    @Test
    public void testNonUniformValues() {
        int namespace = 0x12345678;
        long name = 0x1234567890ABCDEFL;
        UUID namespaceUuid = UuidUtil.namespaceUUID(namespace, name);

        assertEquals(namespace, UuidUtil.namespaceFromUUID(namespaceUuid).intValue());
        assertEquals(name, UuidUtil.nameFromUUID(namespaceUuid).longValue());
    }

    @Test
    public void testNonUniformValuesInTheOtherDirection() {
        int namespace = 0x87654321;
        long name = 0xFEDCBA0987654321L;
        UUID namespaceUuid = UuidUtil.namespaceUUID(namespace, name);

        assertEquals(namespace, UuidUtil.namespaceFromUUID(namespaceUuid).intValue());
        assertEquals(name, UuidUtil.nameFromUUID(namespaceUuid).longValue());
    }
}
