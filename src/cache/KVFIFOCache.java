package cache;
import java.util.LinkedHashMap;
import java.util.Map;

public class KVFIFOCache implements KVCache {
    private final int capacity;
    private LinkedHashMap<String, String> map;
    
    public KVFIFOCache(int cap) {
        capacity = cap;
        map = new LinkedHashMap<String, String>(capacity){
            /**
			 * Supply default serial ID.
			 */
			private static final long serialVersionUID = 2L;

			protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > capacity;
            }
        };
    }

    public synchronized void printAlgorithm() {
        System.out.println("FIFO");
    }
    
    public synchronized void printCache() {
        System.out.println(map);
    }

    public synchronized String get(String key) {
        return map.getOrDefault(key, null);
    }
    
    public synchronized void set(String key, String value) {
        map.put(key, value);
    }
    
    public synchronized void remove(String key) {
        map.remove(key);
    }
}