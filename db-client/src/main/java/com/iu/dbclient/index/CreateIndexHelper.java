package com.iu.dbclient.index;

import com.iu.dbclient.DBConnection;
import com.iu.dbclient.DbConnector;
import com.message.MessageBean;

import java.io.IOException;

import static com.iu.dbclient.DBHelper.*;

public class CreateIndexHelper {

    private static final String CREATE_INDEX_TASK = "createIndex";

    private static final String HASH_INDEX = "hashIndex";

    private static final String BTREE_INDEX = "btree";

    private static final String BPLUSTREE_INDEX = "bplustree";

    private static final String LSMTREE_INDEX = "lsmtree";



    public static String createHashIndex() {
        return createIndex(HASH_INDEX);
    }

    public static String createBTreeIndex() {
        return createIndex(BTREE_INDEX);
    }

    public static String createBPlusTreeIndex() {
        return createIndex(BPLUSTREE_INDEX);
    }

    public static String createLSMTreeIndex() {
        return createIndex(LSMTREE_INDEX);
    }

    private static String createIndex(String indexType) {

        DBConnection connection= null;
        try {
            connection = getConnection();

            return connection.createIndex(new MessageBean(CREATE_INDEX_TASK, indexType));

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (connection!= null)
                connection.close();
        }
        return null;
    }

}
