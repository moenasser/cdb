package com.mnasser.io.cdb.parse;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.proclivitysystems.commons.io.ByteBuilder;

public class HDist implements HDistable {

	public static Log _log = LogFactory.getLog(HDist.class);
	
	public static int hashMod(int mod){  return ( mod < 0 )?  mod %  CDB_SHARD_COUNT :  mod; 	}
	
	public static long _time(long _start){ 	return System.currentTimeMillis() - _start;	}
	public static long _time(){ return System.currentTimeMillis();	}
	
    public static final String use = "usage: hdist  [options] [FILE_PREFIX]  \n\t" +
	"Reads stdin, splits output along several files based on hash of key column.\n\t" +
	"First column is default key column; Tab is default delimiter.\n\n\t" +
	"Produces output files of the format:\n\n\t\t" +
	"FILE_PREFIX.shrd[0-mod]";
    
    //private static Log _log = LogFactory.getLog(HDist.class);
    
    public static void fail(String msg){
		System.err.println(msg);
		System.err.println(use);
		System.exit(1);
    }
	
	@SuppressWarnings("static-access")
	public static Options prepOpts(){
        Options options = new Options();
        
        options.addOption("m","mod",true, "The number of shards to produce.");
        options.addOption("d","delim",true,"Delimiter. '\t' or ',' or '|' ");
        options.addOption("c","col",true,"Column to use as key.");
        options.addOption("p","prefix",true,"Command line option to set the FILE_PREFIX to use");
        options.addOption("s","suffix",true,"Set custom suffix, instead of '.shrd' - don't forget the dot '.' ");
        options.addOption("f","file",true,"The file to read from - otherwise reads from Stdin");
        options.addOption( OptionBuilder.hasArg().withDescription("Skips first n lines.").create("skip"));
        options.addOption("v","verbose",false, "Prints extra info about current run.");
        options.addOption("t","test",false,"Does no work; prints out the file name patterns it will produce and exists.");
        options.addOption("r","reverse",false,"Add in the reverse val->key tuples into the final output");
		
        return options;
	}
	
	public static void main(String[] args) {
		CommandLine cmd = null;
		
		try {
			cmd = new PosixParser().parse( HDist.prepOpts(), args);
		}catch(ParseException pe){
			fail(pe.getMessage());
		}

		HDist hd = new HDist(cmd);
		try {
			hd.run();
		} catch (IOException e) {
			fail(e.getMessage());
		}
		
		System.exit(0);
	}
	
	
	
	private String suffix = ".shrd";
	private char delim = '\t';
	private final String prefix;
	private final int shard_count;
	private boolean verbose = false; 
	private int col = 0;
	private String file = null;
	private InputStream is = System.in;
	private boolean reverse = false;
	private int[] counts;
	private boolean cdbFormatted;


	public String getSuffix()   { return suffix;     }
	public String getPrefix()   { return prefix;     }
	public boolean isReverse()  { return reverse;    }
	public char getDelim()      { return delim;      }
	public int getShard_count()	{ return shard_count;}
	public int getCol() 		{ return col;        }
	public boolean isVerbose() 	{ return verbose;    }
	public String getFile()		{ return file;       }
	public InputStream getIs() 	{ return is;         }
	public boolean isCdbFormatted() {
		return cdbFormatted;
	}

	public void setSuffix(String suffix)      { this.suffix = suffix;   }
	public void setDelim(char delim)          { this.delim = delim;     }
	public void setCol(int col)               { this.col = col;         }
	public void setVerbose(boolean v)         { this.verbose = v;       }
	public void setFile(String fn)	          { this.file = fn;         }
	public void setInputStream(InputStream is){ this.is = is;           }
	public void setReverse(boolean reverse)   { this.reverse = reverse; }
	public void setCdbFormatted(boolean cdbFormatted) {
		this.cdbFormatted = cdbFormatted;
	}
	
	public HDist(String prefix, int shard_count) {
		this.prefix = prefix;
		this.shard_count = shard_count;
		this.counts = new int[this.shard_count];
	}
	
	@SuppressWarnings("unchecked")
	public HDist(CommandLine cmd) {
		List<String> args = cmd.getArgList();
		
		if( ! cmd.hasOption('p') && args.size() == 0 )
			fail("Need output filename prefix!!");
		
		prefix = (cmd.hasOption('p'))? cmd.getOptionValue('p') : args.get(0).trim();
		
		shard_count = (cmd.hasOption("mod"))? Integer.parseInt(cmd.getOptionValue("mod")) : CDB_SHARD_COUNT;
		if( shard_count < 1 )
			fail("Must have positive non-zero shard count");

		// testing what file names would be produced
		if( cmd.hasOption('t')){
			
			for( int ii = 0 ; ii < this.shard_count; ii ++ ){
				String f = this.prefix + this.suffix + ii;
				System.out.println( f );
			}

			System.exit(0);
		}
		
		verbose = cmd.hasOption('v');
		if( verbose )
			_log.info("Will produce "+ shard_count + " shards");
		
		if( cmd.hasOption('s')) 
			suffix = cmd.getOptionValue('s');
		
		if( cmd.hasOption('d')){
			String delim = cmd.getOptionValue('d').trim();
			if( delim.length() > 1 ){
				fail("Must have one-character delimiter!");
			}
			this.delim = delim.charAt(0);
		}
		
		if( cmd.hasOption('c')) {
			try{
				this.col = Integer.parseInt(cmd.getOptionValue('c')) - 1;  // input should start from one; turn into java 0-index
			}catch(NumberFormatException nfe){
				fail(nfe.getMessage());
			}
		}
		
		if( cmd.hasOption('r')) {
			this.reverse = true;
		}
		
		if( cmd.hasOption('f')){
			this.file = cmd.getOptionValue('f');
			prepIS();
		}
		
	}
	
	private void prepIS(){
		if( file != null ){
			File f = new File( this.file  );			
			try {
				this.is = new FileInputStream(f);
			} catch (FileNotFoundException e) {
				fail(e.getMessage());
			}
		}
		
	}
	
	private BufferedOutputStream[] getOpenFileHandles() throws IOException{
		BufferedOutputStream[] fhandles = new BufferedOutputStream[this.shard_count];
		File f = null;
		for( int ii = 0 ; ii < this.shard_count; ii ++ ){
			f =  new File( this.prefix + this.suffix + ii);
			fhandles[ii] = new BufferedOutputStream(new FileOutputStream(f));
			if( this.verbose )
				_log.info("opening file "+ f.getAbsolutePath());
		}
	    return fhandles;
	}

	
	public synchronized void run() throws IOException{
				
		long _start = _time();
		BufferedOutputStream[] files = getOpenFileHandles();
		
		if( verbose ){
			if( delim == '\t') _log.info("Delimiter is TAB");
			else _log.info("Delimiter is " + delim);
			_log.info("Key column is " +  this.col );
			if( is == System.in ) _log.info("reading from stdin ...");
			else _log.info("reading from file...");
		}


		prepIS();
		
		if( is.available() == 0 ){
			try {     Thread.sleep(250);    } catch (InterruptedException e){} 
		}
		
		int cnt = (this.cdbFormatted) ? readFormatted(files): readUnformatted(files);
		
		long time = _time(_start);
		for( int ii = 0; ii <  shard_count ; ii++){
			if (cdbFormatted) {
				files[ii].write('\n');
			}
			files[ii].flush();
			files[ii].close();
		}
		is.close();
		
		if( verbose ){
			_log.info("Done");
			for( int ii =0 ; ii < shard_count; ii++){
				_log.info("Shard " + ii + "\t" + counts[ii]);
			}
			_log.info("Total lines: "+ cnt);
			_log.info("Done in " + ((float)time/1000.0) + " sec");
		}
	}

	//returns int value of byte[] up to length chars
	private static int getInt(byte[] b, int len) {
		int val = 0;
		len -= 1;
		for (int i = 0; i <= len; i++) {
			val += (b[i]-48) * Math.pow(10, len-i);
		}
		return val;
	}
	
	
	
	private static class ByteBuff {
		byte[] array = new byte[256];
		private int count;
		void append(byte b) {
			array[count++] = b;
			if (array.length == count) {
				array = Arrays.copyOf(array, array.length + 256);
			}
		}
	}
	
	//read every line, expecting first chars in format '+n,n:'
	//where n = size of key or value.
	//for cdb, n cannot be a size greater than max 32-bit int, or 4294967296
	private int readFormatted(BufferedOutputStream[] files) throws IOException  {
		BufferedInputStream bis = new BufferedInputStream(this.is);
		ByteBuff buf = new ByteBuff(); //holds +n,n:
		byte[] k = new byte[256]; //holds key - reallocated if necessary
		byte[] n = new byte[10]; //
		int cnt = 0;
		int c = 0;
		while((c = bis.read()) == '+') {
			buf.count = 0;
			int klen = 0, vlen = 0;
			buf.append((byte)'+');
			while ((c = bis.read()) != ',') {
				buf.append((byte)c);
				n[klen++] = (byte)c;
			}
			buf.append((byte)',');
			int ksize = getInt(n, klen);
			if (ksize > k.length) k = new byte[ksize];
			while ((c = bis.read()) != ':') {
				buf.append((byte)c);
				n[vlen++] = (byte)c;
			}
			buf.append((byte)':');
			int vsize = getInt(n, vlen);
			//next chars are key
			for (int i = 0; i < ksize; i++) {
				c = bis.read();
				k[i] = (byte)c;
			}			
			int mod = ByteBuilder.hashCode(k,0,ksize) % shard_count;
			if (mod < 0) {
				mod = mod + shard_count; // Python/corrected modulus
			}
			BufferedOutputStream shard = files[mod]; 
			shard.write(buf.array,0,buf.count);
			shard.write(k,0,ksize);
			shard.write(bis.read()); //'-'
			shard.write(bis.read()); //'>'
			for (int i = 0; i < vsize; i++) {
				shard.write(bis.read());
			}
			shard.write(bis.read()); //'\n'
			counts[mod]++;
			cnt++;
		}
		if (c != -1) {
			//error
			throw new RuntimeException("premature end reading cdb-formatted file " + this.file);
		}
		return cnt;
		
	}

	private int readUnformatted(BufferedOutputStream[] files) throws IOException {
		BufferedInputStream bis = new BufferedInputStream(this.is);
		ByteBuilder bb = new ByteBuilder();
		byte[] key;
		int cnt = 0;

		int idx, mod;
		//String line = null;
		
		int c;
		while( (c = bis.read()) != -1  ){
			
			bb.append((byte)c);
			
			if( c == '\n'){
				cnt++;
				idx = bb.indexOf((byte)delim);
				key = bb.subSequence(0, idx);
				mod = ByteBuilder.hashCode(key) % shard_count;
				
				if (mod < 0) mod = mod + shard_count; // Python/corrected modulus
				files[mod].write( bb.getContent() );
				counts[mod]++;
				
				bb.clear();
			}
		}
		
		return cnt;
		
	}
}
