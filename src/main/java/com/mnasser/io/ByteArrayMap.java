package com.mnasser.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * Acts as a hash map lookup where byte[] are keys. 
 * </br></br>
 * NOTE: <strong>Not thread safe</safe>
 * @author mnasser
 *
 */
@SuppressWarnings("unchecked")
public class ByteArrayMap<V> implements Map<byte[], V>{

	// private int threshold = 70; 
	private int bucketSize = 100;
	private int entries = 0;
	private ArrayList<Entry<V>>[] buckets =  new ArrayList[bucketSize];

	private static class Entry<V>{
		final byte[] key;
		V value;
		
		Entry(byte[] k) {  key = k ; }
		
		public boolean equals(byte[] other){
			return ByteBuilder.equals(key, other);
		}
		@Override
		public boolean equals(Object obj) {
			if( this == obj ) return true;
			if( obj instanceof Entry<?> ){
				Entry<V> e = (Entry<V>)obj;
				return ByteBuilder.equals(key, e.key);
			}
			return false;
		}
	}

	@Override
	public V put(byte[] key, V map){
		int currhash = bucket(key);
		ArrayList<Entry<V>> al = buckets[ currhash ];
		if( al == null ){
			al = new ArrayList<Entry<V>>();
			buckets[ currhash ] = al; 
			bucketSize++;
		}
		
		for( Entry<V> e : al){
			if( e.equals(key)) {
				e.value = map;
				return map; 
			}
		}
		
		Entry<V> e = new Entry<V>(Arrays.copyOf(key,key.length)); /**must copy or else mutability problems*/
		e.value = map;
		al.add(e);
		entries++;
		return map;
	}
	/**
	 * Returns the value stored at this key. Null otherwise.
	 * @param key
	 * @return
	 */
	public V get(byte[] key){
		ArrayList<Entry<V>> al = buckets[ bucket(key) ];
		if( al == null ) return null;
		for( Entry<V> e : al){
			if( e.equals(key)) {
				return e.value;
			}
		}
		return null;
	}
	/**
	 * Returns which bucket index this key should fall under.
	 * @param key
	 * @return
	 */
	int bucket(byte[] key){
		int mod = ByteBuilder.hashCode(key) % buckets.length;
		if (mod < 0) mod = mod + buckets.length; // corrected modulus 
		return mod;
	}
	
	@Override
	public Collection<V> values(){
		LinkedList<V> ll = new LinkedList<V>();
		ArrayList<Entry<V>> curr = null;
		for( int ii = 0 ; ii< buckets.length; ii++){
			curr = buckets[ii];
			if( curr != null && ! curr.isEmpty()){
				for( Entry<V> e : curr){
					ll.add(e.value);
				}
			}
		}
		return ll;
	}
	
	@Override
	public void clear() {
		buckets = new ArrayList[100];
		bucketSize = 0;
		entries = 0;
		System.gc(); // needed? 
	}
	/**
	 * Determines if a key is present 
	 * @param key
	 * @return
	 */
	public boolean containsKey(byte[] key) {
		ArrayList<Entry<V>> al = buckets[ bucket(key) ];
		if( al == null || al.isEmpty() ) return false;
		for( Entry<V> e : al){
			if( e.equals(key)) {
				return true;
			}
		}
		return false;
	}
	/**
	 * Removes an entry.
	 * NOTE thread safe.
	 * @param key
	 * @return
	 */
	public V remove(byte[] key) {
		int b = bucket(key);
		ArrayList<Entry<V>> al = buckets[ b ];
		if( al == null ) return null;
		Entry<V> e;
		for( int ii = 0, len = al.size(); ii < len; ii++){
			e = al.get(ii);
			if( e.equals(key)) {
				al.remove(ii);
				entries--;
				if ( al.isEmpty() ){
					al = null;
					buckets[ b ] = null; // ?
					bucketSize--;
				}
				return e.value;
			}
		}
		return null;
	}
	@Override
	public boolean containsValue(Object value) {
		throw new RuntimeException("Unsupported Method Exception");
	}
	@Override
	public Set<java.util.Map.Entry<byte[], V>> entrySet() {
		throw new RuntimeException("Unsupported Method Exception");
	}
	@Override
	public V get(Object key) {
		if( key instanceof byte[] )  return get((byte[])key);
		throw new RuntimeException("Invalid key type. Must be byte array.");
	}
	@Override
	public boolean isEmpty() {
		return entries == 0;
	}
	@Override
	public Set<byte[]> keySet() {
		throw new RuntimeException("Unsupported Method Exception");
	}
	@Override
	public void putAll(Map<? extends byte[], ? extends V> m) {
		for( byte[] k : m.keySet() ){
			this.put(k, m.get(k));
		}
	}
	@Override
	public V remove(Object key) {
		if( key instanceof byte[])  return remove((byte[])key);
		throw new RuntimeException("Invalid key type. Must be byte array.");
	}
	@Override
	public int size() {
		return entries;
	}
	@Override
	public boolean containsKey(Object key) {
		if( key instanceof byte[])  return containsKey((byte[])key);
		throw new RuntimeException("Invalid key type. Must be byte array.");
	}
}
