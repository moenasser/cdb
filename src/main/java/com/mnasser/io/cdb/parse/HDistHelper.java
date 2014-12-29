package com.mnasser.io.cdb.parse;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.proclivitysystems.commons.io.cdb.Cdb;
import com.proclivitysystems.filehandlers.Utils;

/**
 * Helps with common functions on sharded CDBs.
 * 
 * @author mnasser
 */
public class HDistHelper implements HDistable {

	public static Log _log = LogFactory.getLog(HDistHelper.class); 

	/**
	 * Dumps sharded CDB contents into one sorted dump file. 
	 * Given the location of a HDist'ed CDB folder, will
	 * dump/sort each CDB and merge sort into dumpFileName.</br>
	 * 
	 * Looks for shards named :
	 * <pre>
	 * 	dir/cdbPrefix*shrd*cdb
	 * </pre>
	 *  
	 * Sets LC_ALL=C to use traditional ascii sorting.
	 * 
	 * @param dir	Location of the mapdata folder
	 * @param cdbPrefix	The prefix that is being used for the shards.
	 * @param dumpFileName name of dump file.
	 */
	public static void dumpCdb(File dir, String cdbPrefix, File dumpFile) {
		long start = System.currentTimeMillis(); 
		
		String base_name = dir.getAbsolutePath() + '/'+cdbPrefix ;
		
		String dmp_sort_cmd = "export LC_ALL=C;\n" +
				"SEP=`echo a | sed 's/a/\\x01/'`;\n" +
				"for ff in `ls -1 "+base_name+"*shrd*cdb`;do\n" +
				"echo Dumping $ff\n" +		//CDB keys & values are separated by sapce ' '   
				"cdb -d $ff | sed 's/+//' | sed 's/:/\\x01/' | awk -F \"\\x01\" 'BEGIN{OFS=\"\\x01\"}{if($0==\"\")next; split($1,lengths,\",\"); key=substr($2,1,lengths[1]); val = substr($2,lengths[1]+3,lengths[2]); print key,val}'|  sort -T/sort -k 1,1 -t $SEP > $ff.sort.tmp &\n" +		
				"done\n" +
				"wait";
		String merg_sort_cmd = "export LC_ALL=C;\n" +
		        "SEP=`echo a | sed 's/a/\\x01/'`;\n" +
				"for ff in `ls -1 "+base_name+"*shrd*cdb.sort.tmp`;do\n" +
				"cmd=\"$ff $cmd\" \n" +
				"done\n" +		// Turn space into comma ',' to keep with legacy MappingServer convention
				"echo Merge sorting $cmd files into "+dumpFile.getAbsolutePath()+"\n" +
				"sort -T/sort -m -k 1,1 -t $SEP $cmd | sed 's/\\x01/,/'  > " + dumpFile.getAbsolutePath() + "\n" +
				"rm "+base_name+"*cdb.sort.tmp;";

		Utils.shellOut(_log, dmp_sort_cmd);
		Utils.shellOut(_log, merg_sort_cmd);
		
		long end = System.currentTimeMillis();
		_log.info("Time to dump/sort contents: " + (end-start)/1000.0 + " sec");
	}

	
	
	
	/**
	 * Returns number of records in sharded CDBs
	 * 
	 * @param cdbs
	 * @return total size of records on all shards
	 */
	public static int getCdbSize(Cdb[] cdbs){
		long start = System.currentTimeMillis(); 
		
		StringBuilder cdbfiles = new StringBuilder();
		for( Cdb c : cdbs ){
			cdbfiles.append(c.getPath()).append(' ');
		}
		
		int size = 0;
		
		String cmd = "for ff in " + cdbfiles.toString() + "; do\n" +
				"cdb -s $ff | head -1 | sed 's/[^0-9]//g' & \n" +
				"done;\n" +
				"wait";
		
		for( String records : Utils.shellOut( cmd ) ){
			size += Integer.parseInt(records);
		}
		
		
		long end = System.currentTimeMillis();
		
		_log.info("Total records : " +  size);
		_log.info("Time to stat all cdb shards : " + (end-start)/1000.0 + " sec");
		
		return size;
	}
	
	/**
	 * Given src file, will HDist the content and create several CDBs.
	 * TarGz's the CDB files.</br>
	 * 
	 * Generates CDBs of the form:
	 * <pre>
	 * 	dir/cdbPrefix.shrd[n].cdb
	 * </pre>
	 * 
	 * <bold>NOTE:</bold> Assumes input key/value delimiter is comma ',' 
	 * 
	 * @param dir	Folder where to place cdbs
	 * @param cdbPrefix Prefix to name the cdb's by.
	 * @param inputFile Src file. 
	 * @return	Name of tarball file produced. </br>
	 * 		Usually of the form <strong>dir/mapData.{cdbPrefix}.cdb_shards.tar.gz</strong>
	 * @throws IOException
	 */
	public static String makeCdb(File dir, String cdbPrefix, File inputFile) throws IOException{
		return makeCdb(dir, cdbPrefix, inputFile, false);	
	}
	

	public static String makeCdb(File dir, String cdbPrefix, File inputFile,boolean reverse ) throws IOException{
		long _start = System.currentTimeMillis();
		
		String base_name = dir.getAbsolutePath() + '/'+cdbPrefix ;
		
		_log.info("Creating " + base_name + " from dump file ... ");
		
		char delim = ',';
		HDist hd = new HDist( base_name , CDB_SHARD_COUNT);
		hd.setInputStream( new FileInputStream(inputFile));
		boolean cdbFormatted = false;
		
		if (inputFile.exists()) {
			//test is file is already in cdb format
			String firstLine = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(inputFile)))).readLine();
			if (firstLine != null) {
				int idx = firstLine.indexOf(':');
				if (idx != -1) {
					cdbFormatted =  Pattern.compile("\\+\\d+\\,\\d+:").matcher(firstLine.substring(0,idx+1)).matches();
				}
			}
		}
		
		hd.setDelim(delim);
		hd.setVerbose(true);
		hd.setReverse(reverse);
		hd.setCdbFormatted(cdbFormatted);
		
		
		try {
			
			//Utils.shellOut(_log,"/psapp/sbin/rebuild_cdb_from_dump "+  inputFile.getAbsoluteFile().toString()  + ' ' + base_name);
			hd.run();
			
		}catch(IOException ioe){
			// remove the shrds in case of failure
			Utils.shellOut(_log, "rm " + base_name + "*shrd?");
			throw ioe;
		}
		
		String cmd =  
		"export LC_ALL=C; \n" +
		"for ff in `ls -1 "+base_name+"*shrd?`;do \n" +
		"cat $ff ";
		if (!cdbFormatted) {
			cmd += "| sed 's/,/\\x01/' " +
			"| awk -F \"\\x01\" 'BEGIN{vlen=0;klen=0}{klen=length($1);vlen=length($2);print \"+\"klen\",\"vlen\":\"$1\"->\"$2}END{print \"\"}' ";
		}
		 cmd += "| cdb -c $ff.cdb && rm $ff && echo $ff.cdb &\n" +
		"done\n" +
		"wait";

		Utils.shellOut(_log, cmd);
		
		
		_log.info("Built CDBs.");
		_log.info("Creating tar of CDBs...");
		
		String targzfn = dir.getAbsolutePath() + "/mapData."+cdbPrefix+".cdb_shards.tar";
		
		cmd = "for ff in `ls -1 "+base_name+"*shrd*.cdb`;do \n" +
		"gzip -c $ff > $ff.gz & \n" + 
		"done\n" +
		"wait\n" +
		"cd "+dir.getAbsolutePath()+" && tar -cf   "+targzfn+" *shrd*.cdb.gz --remove-files   2>/dev/null ";

		//Utils.shellOut(_log, "cd "+dir.getAbsolutePath()+" && tar -czf   "+targzfn+" *.cdb   2>/dev/null ");
		Utils.shellOut(_log, cmd );
		
		long _end = System.currentTimeMillis();
		
		_log.info("Done building " + base_name + " in " + ( (float)(_end - _start)/1000.0) +  " sec" ) ;
		
		return targzfn;
	}
	
	
	/**
	 * Updates existing dump file with newer key/values in delta.
	 *  
	 * @param delta
	 * @param cdbDump
	 * @param dest
	 */
	public static void mergeDelta(String delta, String cdbDump, String dest){
		long start = System.currentTimeMillis();
		
		String delta_sort = delta+".sort";
		String sortDelta = "export LC_ALL=C;\n" +
				"echo 'Sorting delta file'\n" +		//Deltas come in comma separated ','
				"sort -k 1,1 -t ',' "+delta+"   > " + delta_sort;
		String mergeSort = "export LC_ALL=C;\n" +
				"echo 'Merging dump and delta files ...'\n" +
				"sort -k 1,1 -t ',' -mu "+delta_sort +"  "+cdbDump+" > "+dest+"\n" +
				"rm "+delta_sort;
		
		/***
		### Sort the delta file
		echo 'Sorting delta file'
	    safe sort -k 1,1 -t $'\t' $2 > $2.sort
	    
	    ### Merge sorting 
	    echo 'Merging dump and delta files ...'
	    safe sort -k 1,1 -t $'\t' -mu $2.sort $1  >   $3.new  
	    rm $2.sort
	    ****/
		
		Utils.shellOut(_log, sortDelta);
		Utils.shellOut(_log, mergeSort);
		
		_log.info("Megred Dump and Delta in " + ( ((float)System.currentTimeMillis()-start) / 1000.0 ) + " sec ");
	}

}
