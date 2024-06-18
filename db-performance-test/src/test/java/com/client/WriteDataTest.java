//package com.client;
//
//import com.iu.dbclient.DBConnection;
//import com.iu.dbclient.DbConnector;
//import com.message.MessageBean;
//import org.json.JSONObject;
//
//import java.io.*;
//
///**
// * see here how to write and read serialized object to sockets
// */
//public class WriteDataTest {
//    public static void main(String... args) throws IOException {
//
//        for (int i =0; i <100; i++) {
//            String result = writeRecord(i);
//
//            System.out.println("Result " + result);
//        }
//
////        writeRecord(104);
//
//    }
//
//
//    private static String writeRecord(int i) {
//        String taskType = "append";
//
//        DBConnection connection= null;
//            try {
//
//                String payload = new JSONObject()
//                        .put("data", "testdata" + 1)
//                        .put("id", i).toString() ;
//
//                connection = DbConnector.INSTANCE.getConnection("localhost", 5555);
//
//                return connection.appendData(new MessageBean(taskType, payload));
//            } catch (IOException | ClassNotFoundException e) {
//                e.printStackTrace();
//            } finally {
//                if (connection!= null)
//                    connection.close();
//            }
//
//            return null;
//        }
//}
