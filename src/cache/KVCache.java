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
     * Delete a key in the cache
     */
	public void delete(String key);

     /*
      * Print the algorithm used
      */
     public void printAlgorithm();

     /*
      * Print the cache 
      */
     public void printCache();
}
