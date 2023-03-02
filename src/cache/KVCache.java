package cache;

public interface KVCache {
	/**
     * Get the value associated with the given key
     * @return  string value
     */
	public String get(String key);
	
	/**
     * Set the given cache key to the given value
     */
	public void set(String key, String value);
    
     /*
     * Get the size of the cache
     */
     public int getSize();
	
     /*
     * Delete a key in the cache
     */
	public void remove(String key);

     /*
      * Print the algorithm used
      */
     public void printAlgorithm();

     /*
      * Print the cache 
      */
     public void printCache();
}
