//package com.client;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.iu.dbclient.DBConnection;
//import com.iu.dbclient.DbConnector;
//import com.message.MessageBean;
//import org.json.JSONObject;
//import org.junit.Test;
//
//import java.io.IOException;
//
//import static org.junit.Assert.*;
//
///**
// * see here how to write and read serialized object to sockets
// * https://stackoverflow.com/questions/27736175/how-to-send-receive-objects-using-sockets-in-java
// */
//public class FindDataTest {
//
//    @Test
//    public void findRecordWithoutIndex(){
//        testFindDataWithAndWithoutIndexes(9, "none", "9", "testdata1");
//    }
//
//    @Test
//    public void findRecordWithHashIndex(){
//        testFindDataWithAndWithoutIndexes(9, "hashIndex", "9", "testdata1");
//    }
//
//    @Test
//    public void findRecordWithBTree(){
//        testFindDataWithAndWithoutIndexes(9, "btree", "9", "testdata1");
//    }
//
//    @Test
//    public void findRecordWithBPlusTree(){
//        testFindDataWithAndWithoutIndexes(9, "bplustree", "9", "testdata1");
//    }
//
//    @Test
//    public void findRecordWithLSMTree(){
//        testFindDataWithAndWithoutIndexes(9, "lsmtree", "9", "testdata1");
//    }
//
//    private void testFindDataWithAndWithoutIndexes(Integer id, String indexType, String expectedId, String expectedData) {
//        String result = readData(id, indexType);
//        assertNotNull(result);
//        assertTrue(result.contains(String.valueOf(id)));
//
//        String resId = getDataFromJsonStrByKey(result.substring(2), "id");
//        assertNotNull(resId);
//        assertEquals(expectedId, resId);
//        String resData = getDataFromJsonStrByKey(result.substring(2), "data");
//        assertNotNull(resData);
//        assertEquals(expectedData, resData);
//    }
//
//    private String getDataFromJsonStrByKey(String jsonStr, String key) {
//        ObjectMapper objectMapper = new ObjectMapper();
//        try {
//            // Convert String to JSON Object
//            JsonNode jsonNode = objectMapper.readTree(jsonStr);
//            System.out.println(jsonNode.toString());
//            return jsonNode.get(key).asText();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
//
//    private static String readData(int id, String indexType) {
//        String taskType = "find";
//        DBConnection connection= null;
//            try {
//                connection = DbConnector.INSTANCE.getConnection("localhost", 5555);
//
//                String payload = new JSONObject()
//                        .put("indexType", indexType)
//                        .put("id", id).toString() ;
//
//                return connection.readData(new MessageBean(taskType, payload));
//
//            } catch (IOException | ClassNotFoundException e) {
//                e.printStackTrace();
//            } finally {
//                if (connection!= null)
//                    connection.close();
//            }
//
//            return null;
//    }
//}
