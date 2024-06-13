package com.iu.indexes;

public interface Index {

    /**
     * search in index by key
     * @param key
     * @return
     */
    public Object search(int key);

    /**
     * insert into index
     * @param key key in index
     * @param value physical address
     */
    public void insert(int key, Object value);

//    todo add delete method

}
