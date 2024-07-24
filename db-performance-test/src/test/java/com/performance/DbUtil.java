package com.performance;

import com.iu.dbclient.DBConnection;
import com.iu.dbclient.DbConnector;
import com.message.MessageBean;
import org.json.JSONObject;

import java.io.IOException;

public class DbUtil {

    private static final String DB_HOST = "localhost";
    private static final int DB_PORT = 5555;

    public static String execute(String taskType, String payload){
        DBConnection connection= null;
        try {
            connection = DbConnector.INSTANCE.getConnection(DB_HOST, DB_PORT);

            return connection.appendData(new MessageBean(taskType, payload));
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (connection!= null)
                connection.close();
        }

        return null;
    }

    public static void generateTestData() {
        for (int i =0; i < 100; i++) {
            String payload = new JSONObject()
                    .put("data", "testdata" + i)
                    .put("id", i).toString();
            String result = execute("append", payload);

            System.out.println("Result " + result);
        }
    }

    public static String removeDB() {
        return execute("deleteDb", "deleteDb");
    }
}
