package com.client;

import com.iu.dbclient.DBConnection;
import com.iu.dbclient.DbConnector;
import com.message.MessageBean;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

/**
 * see here how to write and read serialized object to sockets
 */
public class CreateIndexTest {

    @Test
    public void testCreateHashIndexTask(){
        String indexType = "hashIndex";
        String result = createIndex(indexType);

        System.out.println("Result " + result);

        assertNotNull(result);
    }

    @Test
    public void testCreateBTreeTask(){
        String indexType = "btree";
        String result = createIndex(indexType);

        System.out.println("Result " + result);

        assertNotNull(result);
    }

    @Test
    public void testCreateBPlusTreeTask(){
        String indexType = "bplustree";
        String result = createIndex(indexType);

        System.out.println("Result " + result);

        assertNotNull(result);
    }


//    public static void main(String... args) throws IOException {
//
//        String indexType = "hashIndex";
//        String result = createIndex(indexType);
//
//        System.out.println("Result " + result);
//
//        indexType = "btree";
//
//        result = createIndex(indexType);
//
//        System.out.println("Result of creating BTreeIndex" + result);
//
////        indexType = "bplustree";
////
////        result = createIndex(indexType);
////
////        System.out.println("Result of creating BPlusTreeIndex " + result);
////
////        indexType = "lsmtree";
////
////        result = createIndex(indexType);
////
////        System.out.println("Result of creating LSMTree " + result);
//    }

    private static String createIndex(String indexType) {
        String taskType = "createIndex";

        DBConnection connection= null;
            try {
                connection = DbConnector.INSTANCE.getConnection("localhost", 5555);

                return connection.createIndex(new MessageBean(taskType, indexType));

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            } finally {
                if (connection!= null)
                    connection.close();
            }
            return null;
        }
}
