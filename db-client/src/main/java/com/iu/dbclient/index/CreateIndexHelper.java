package com.iu.dbclient.index;

import com.iu.dbclient.DBConnection;
import com.message.MessageBean;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.dbclient.DBHelper.DB_PORT;
import static com.iu.dbclient.DBHelper.DB_URL;

/**
 * Convenience facade for creating and deleting indexes from client code.
 * Each method opens its own connection (fire-and-forget — see DBConnection).
 */
public class CreateIndexHelper {

    private static final Logger LOGGER = Logger.getLogger(CreateIndexHelper.class.getName());

    private static final String CREATE_INDEX_TASK = "createIndex";
    private static final String DELETE_INDEX_TASK = "deleteIndex";

    public static String createHashIndex()     { return createIndex("hashIndex"); }
    public static String createBTreeIndex()    { return createIndex("btree"); }
    public static String createBPlusTreeIndex(){ return createIndex("bplustree"); }
    public static String createLSMTreeIndex()  { return createIndex("lsmtree"); }
    public static String createGINIndex()      { return createIndex("gin"); }
    public static String createBitmapIndex()   { return createIndex("bitmap"); }

    public static String deleteHashIndex()     { return deleteIndex("hashIndex"); }
    public static String deleteBTreeIndex()    { return deleteIndex("btree"); }
    public static String deleteBPlusTreeIndex(){ return deleteIndex("bplustree"); }
    public static String deleteLSMTreeIndex()  { return deleteIndex("lsmtree"); }
    public static String deleteGINIndex()      { return deleteIndex("gin"); }
    public static String deleteBitmapIndex()   { return deleteIndex("bitmap"); }

    private static String createIndex(String indexType) {
        return send(CREATE_INDEX_TASK, indexType);
    }

    private static String deleteIndex(String indexType) {
        return send(DELETE_INDEX_TASK, indexType);
    }

    private static String send(String taskType, String indexType) {
        DBConnection connection = new DBConnection(DB_URL, DB_PORT);
        try {
            return connection.send(new MessageBean(taskType, indexType));
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Failed to send " + taskType + " for " + indexType, e);
            return null;
        }
    }
}
