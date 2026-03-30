package com.iu.dbclient.search;

import com.iu.dbclient.DBConnection;
import com.message.MessageBean;
import org.json.JSONObject;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.dbclient.DBHelper.DB_PORT;
import static com.iu.dbclient.DBHelper.DB_URL;

/**
 * Convenience facade for document lookups.
 *
 * indexType values: "none", "hashIndex", "btree", "bplustree", "lsmtree", "gin", "bitmap"
 */
public class FindDataHelper {

    private static final Logger LOGGER = Logger.getLogger(FindDataHelper.class.getName());

    private static final String FIND_TASK  = "find";
    private static final String ID_FIELD   = "id";
    private static final String INDEX_TYPE = "indexType";

    public static String readDataWithoutIndexes(int id) {
        return readData(id, "none");
    }

    public static String readDataWithIndex(int id, String indexType) {
        return readData(id, indexType);
    }

    private static String readData(int id, String indexType) {
        DBConnection connection = new DBConnection(DB_URL, DB_PORT);
        try {
            String payload = new JSONObject()
                    .put(INDEX_TYPE, indexType)
                    .put(ID_FIELD, id)
                    .toString();
            return connection.send(new MessageBean(FIND_TASK, payload));
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Find failed for id=" + id + " indexType=" + indexType, e);
            return null;
        }
    }
}
