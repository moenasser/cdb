package com.mnasser.io.cdb.client;

import java.io.IOException;

public interface MapLookup {

	public abstract String getHost();

	public abstract int getPort();

	/**
	 * Establishes a connection to the host/port and prepares i/o channels 
	 */
	public abstract void connect();

	public abstract boolean isConnected();

	/**
	 * Closes input/output and socket connections
	 */
	public abstract void close();

	/**
	 * Constructs proper request and queries remote map for key.
	 * @param mi MapInfo containing map folder and map type.
	 * @param key Key whose value to retrieve.
	 * @return Value associated with this key if there is one.
	 * @throws IOException
	 */
	public abstract byte[] lookup(MapInfo mi, byte[] key) throws IOException;

	/**
	 * Constructs proper binary request and queries remote map for key, returning all values
	 * @param mapName
	 * @param mapType
	 * @param key
	 * @return result set as byte[]
	 * @throws Exception
	 */
	public abstract byte[] lookupAll(MapInfo mi, byte[] key) throws IOException;

	@Deprecated
	public abstract byte[] updateMap(MapInfo mi, String path, boolean full)
			throws IOException;

}