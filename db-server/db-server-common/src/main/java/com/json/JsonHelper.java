package com.json;

import org.json.JSONObject;

public class JsonHelper {
    public static Object getValueFromJsonByKey(final String jsonStr, final String key) {
        JSONObject jsonObj = new JSONObject(jsonStr);

        return jsonObj.get(key);
    }

}
