package com.iu.dbclient.search;

import com.iu.dbclient.DBConnection;
import com.message.MessageBean;
import org.json.JSONObject;

import java.io.IOException;

import static com.iu.dbclient.DBHelper.getConnection;

public class FindDataHelper {

    private static final String FIND_TASK = "find";
    private static final String ID_FIELD = "id";
    private static final String INDEX_TYPE = "indexType";

    public static String readDataWithoutIndexes(int id){
        return readData(id, "none");
    }

    public static String readDataWithIndex(int id, String indexType){
        return readData(id, indexType);
    }

    private static String readData(int id, String indexType) {
        DBConnection connection= null;
        try {
            connection = getConnection();

            String payload = new JSONObject()
                    .put(INDEX_TYPE, indexType)
                    .put(ID_FIELD, id).toString() ;

            return connection.readData(new MessageBean(FIND_TASK, payload));

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (connection!= null)
                connection.close();
        }

        return null;
    }
}
