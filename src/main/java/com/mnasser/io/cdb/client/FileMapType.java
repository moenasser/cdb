package com.mnasser.io.cdb.client;

/**
 * The accepted types of maps that the CDBLookupService can work with.
 * 
 * By default everything is really just a blob key to a blob value.
 * 
 * However, with Type-2 dimensions an array of values is returned. A Type-2
 * dimension in analytics speak is a a key with a datestamp. For example your
 * physical address where you live might change over time. Your phone number
 * might change over time.  Your credit card number might change over time.
 * You get the idea.  
 * 
 * Since the service knows of the type 2 nature of such a map the service can 
 * be tipped off to split the values and return the value that is aligned to
 * a given date range.
 *
 */
public enum FileMapType
{
    UNKNOWN(-1),
    STRING_TO_STRING_CDB(1),
    STRING_TO_STRING_DATED_CDB(2);

    
    int type = -1;
    
    FileMapType(int i)
    {
        type = i;
    }

    
    
    public int asInt()
    {
        return type;
    }

    /**
     * Given the enum type name of a map implementation, returns the
     * specific FileMapTyoe of this class. Returns UNKNOWN otherwise.
     * 
     * @param className
     * @return
     */
    public static FileMapType fromClassName(String className){
    	if( "StringToStringDatedMapper".equals(className)){
    		return STRING_TO_STRING_DATED_CDB;
    	}else if("StringToStringMapper".equals(className)){
    		return STRING_TO_STRING_CDB;
    	}
    	return UNKNOWN;
    }
    
    /**
     * Given the int type of map returns the requesite FileMapType
     * enum instance.
     * 
     * @param intType
     * @return
     */
    public static FileMapType fromInt(int intType)
    {
        for (FileMapType self : values())
        {
            if (self.asInt() == intType)
                return self;
        }
        return UNKNOWN;
    }
    
}
