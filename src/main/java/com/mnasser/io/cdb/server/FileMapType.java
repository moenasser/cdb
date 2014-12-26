/**
 * Copyright (c) 2009, Proclivity Systems or third-party contributors as
 * indicated by the @author tags or express copyright attribution
 * statements applied by the authors. All rights reserved.
 * http://www.proclivitysystems.com
 *
 * This copyrighted material is proprietary and confidential information and distribution
 * without a license is prohibited.
 *
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in
 * the documentation and/or other materials provided with the
 * distribution.
 *
 * 3. Redistributions of any form whatsoever must retain the following
 * acknowledgment:
 * "This product includes software developed by the Proclivity Systems
 * and contains proprietary and confidential information."
 *
 * THIS SOFTWARE IS PROVIDED BY THE PROCLIVITY SYSTEMS "AS IS" AND ANY
 * EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE PROCLIVITY SYSTEMS OR
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
 * OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.mnasser.io.cdb.server;

/**
 * Enumeration of the accepted file mapper types that CDBLookupService works with.
 * @author alaw
 *
 */
public enum FileMapType
{
    UNKNOWN(-1),
    /*
    INT_TO_INT(1),
    STRING_TO_INT(2),
    INT_TO_MULTI_INT(3),
    COOKIE_TO_INT(4),
    INT_TO_BYTE(5),
    DOUBLELONG_TO_INT(6),
    LONG_TO_BOOL(7),
    LONG_TO_LONG(8),
    DATED_STRING_TO_LONG(9),
	*/
	
    STRING_TO_STRING_CDB(10),
    STRING_TO_STRING_DATED_CDB(11);

    
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
     * Given the classname of FileMapper implementation, returns the
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
     * Neat function! 
     * 
     * @param intType
     * @return
     * @author alaw
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
