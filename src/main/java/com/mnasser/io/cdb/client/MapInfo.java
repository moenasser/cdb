package com.mnasser.io.cdb.client;



/**
 * Dummy holder of map folder name and type.
 * <p>
 * The CDB files that will be constructed are spread over several mmap'd files
 * on disk. Within a folder
 * Builds out and holds the query prefix for fast query construction.
 * 
 * @author mnasser
 */
public class MapInfo {

	private final String mapName;
	private final int mapType;
	
	private final byte[] queryPrefix;
	
	public MapInfo(String m, int i) {
		if( i < 0 || FileMapType.fromInt(i) == FileMapType.UNKNOWN )
			throw new BadMapTypeException(i);
		mapName = m;
		mapType = i;
		queryPrefix = CdbMapLookup.buildQueryPrefix(mapName, mapType);
	}
	public MapInfo(String m, FileMapType t) {
		this(m,t.asInt());
	}
	/**
	 * Creates a MapInfo with a default FileMapType.
	 * This is ok for remote requests involving kill, stat
	 * and update requests; but shouldn't be used for query requests.
	 * @param m
	 */
	public MapInfo(String m){
		this(m, FileMapType.STRING_TO_STRING_CDB); // default. Good for kill/stat/update commands
	}
	
	public static class BadMapTypeException extends RuntimeException{
		private static final long serialVersionUID = 1L;
		private final int i;
		public BadMapTypeException(int i) {
			this.i = i;
		}
		@Override
		public String getMessage() {
			return "Attempt to use an unknown map type : " + i ;
		}
	}
	
	public String getMapName()    {  return mapName;    }
	public int getMapType()       {  return mapType;    }
	public byte[] getQueryPrefix(){ return queryPrefix; }

	@Override
	public String toString() {
		return "MapInfo [mapName=" + mapName + ", mapType=" + mapType + "]";
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((mapName == null) ? 0 : mapName.hashCode());
		result = prime * result + mapType;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MapInfo other = (MapInfo) obj;
		if (mapName == null) {
			if (other.mapName != null)
				return false;
		} else if (!mapName.equals(other.mapName))
			return false;
		if (mapType != other.mapType)
			return false;
		return true;
	}

}
