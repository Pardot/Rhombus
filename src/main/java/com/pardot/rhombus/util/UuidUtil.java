package com.pardot.rhombus.util;

import com.datastax.driver.core.utils.UUIDs;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Date;
import java.util.UUID;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 5/6/13
 *
 UUID                   = time-low "-" time-mid "-"
 						  time-high-and-version "-"
 						  clock-seq-and-reserved
 						  clock-seq-low "-" node
 time-low               = 4hexOctet
 time-mid               = 2hexOctet
 time-high-and-version  = 2hexOctet
 clock-seq-and-reserved = hexOctet
 clock-seq-low          = hexOctet
 node                   = 6hexOctet
 hexOctet               = hexDigit hexDigit
 hexDigit =
 "0" / "1" / "2" / "3" / "4" / "5" / "6" / "7" / "8" / "9" /
 "a" / "b" / "c" / "d" / "e" / "f" /
 "A" / "B" / "C" / "D" / "E" / "F"

 */
public class UuidUtil {

	/**
	 * Generate a type 3 namespace uuid from an integer namespace and name
	 * @param namespace Integer representing the namespace
	 * @param name Integer representing the name
	 * @return Type 3 UUID built from the namespace and name
	 */
	public static UUID namespaceUUID(Integer namespace, Integer name) {
		//Put our namespace and name into a bytebuffer
		ByteBuffer nameBuffer = ByteBuffer.allocate(8);
		nameBuffer.putInt(namespace);
		nameBuffer.putInt(name);

		//Create our msb and lsb return buffers
		ByteBuffer msb = ByteBuffer.allocate(8);
		ByteBuffer lsb = ByteBuffer.allocate(8);

		//Set octets 0-3 of time_low to octets 0-3 of namespace+name
		msb.put(0, nameBuffer.get(0));
		msb.put(1, nameBuffer.get(1));
		msb.put(2, nameBuffer.get(2));
		msb.put(3, nameBuffer.get(3));

		//Set the four most significant bits of the time_hi_and_version field to the 4 bit version number
		//(00110000 = 48)
		msb.put(7, (byte) 48);

		//Set octets 0-3 of the node field to octets 4-7 of namespace+name
		lsb.put(2, nameBuffer.get(4));
		lsb.put(3, nameBuffer.get(5));
		lsb.put(4, nameBuffer.get(6));
		lsb.put(5, nameBuffer.get(7));

		//Set the two most significant bits to 01 (01000000 = 64)
		lsb.put(1, (byte)64);

		return new UUID(msb.getLong(), lsb.getLong());
	}

	/**
	 * Retrieve the integer namespace from a namespace uuid generated using this class
	 * @param uuid UUID generated using
	 * @return Namespace retrieved from the UUID
	 */
	public static Integer namespaceFromUUID(UUID uuid) {
		ByteBuffer msb = ByteBuffer.allocate(8);
		msb.putLong(uuid.getMostSignificantBits());
		ByteBuffer namespace = ByteBuffer.allocate(4);
		namespace.put(0, msb.get(0));
		namespace.put(1, msb.get(1));
		namespace.put(2, msb.get(2));
		namespace.put(3, msb.get(3));

		return namespace.getInt();
	}

	/**
	 * Retrieve the integer name from a namespace uuid generated using this class
	 * @param uuid UUID generated using
	 * @return Name retrieved from the UUID
	 */
	public static Integer nameFromUUID(UUID uuid) {
		ByteBuffer lsb = ByteBuffer.allocate(8);
		lsb.putLong(uuid.getLeastSignificantBits());
		ByteBuffer name = ByteBuffer.allocate(4);
		name.put(0, lsb.get(2));
		name.put(1, lsb.get(3));
		name.put(2, lsb.get(4));
		name.put(3, lsb.get(5));

		return name.getInt();
	}

	static final long NUMBER_OF_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

	public static Date getDateFromUUID(UUID uuid) {
		Long time =  (uuid.timestamp() - NUMBER_OF_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
		return new Date(time);
	}


}
