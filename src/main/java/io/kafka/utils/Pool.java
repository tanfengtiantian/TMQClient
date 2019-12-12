package io.kafka.utils;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
/**
 * @author tf
 * @version 创建时间：2019年1月31日 下午3:02:18
 * @ClassName Pool
 */
public class Pool<K extends Comparable<K>, V> implements Map<K, V> {

    private final ConcurrentMap<K, V> pool = new ConcurrentSkipListMap<K, V>();

    public int size() {
        return pool.size();
    }

    public boolean isEmpty() {
        return pool.isEmpty();
    }

    public boolean containsKey(Object key) {
        return pool.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return pool.containsValue(value);
    }

    public V get(Object key) {
        return pool.get(key);
    }

    public V put(K key, V value) {
        return pool.put(key, value);
    }

    public V putIfNotExists(K key,V value) {
        return pool.putIfAbsent(key, value);
    }
    public V remove(Object key) {
        return pool.remove(key);
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        pool.putAll(m);
    }

    public void clear() {
        pool.clear();
    }

    public Set<K> keySet() {
        return pool.keySet();
    }

    public Collection<V> values() {
        return pool.values();
    }


    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return pool.entrySet();
    }
    
    @Override
    public String toString() {
        return pool.toString();
    }

}