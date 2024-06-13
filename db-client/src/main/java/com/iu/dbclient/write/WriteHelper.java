package com.iu.dbclient.write;

import com.iu.dbclient.DBConnection;
import com.message.MessageBean;
import org.json.JSONObject;

import java.io.IOException;

import static com.iu.dbclient.DBHelper.getConnection;

public class WriteHelper {

    private static final String APPEND_TASK = "append";

    public static String writeRecord(int id) {

        DBConnection connection= null;
        try {

            String payload = new JSONObject()
                    .put("data", "testdata" + 1)
                    .put("id", id).toString() ;

            connection = getConnection();

            return connection.appendData(new MessageBean(APPEND_TASK, payload));
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (connection!= null)
                connection.close();
        }

        return null;
    }
}
