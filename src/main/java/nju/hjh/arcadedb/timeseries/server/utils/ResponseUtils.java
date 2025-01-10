package nju.hjh.arcadedb.timeseries.server.utils;

import java.util.HashMap;
import java.util.Map;

public class ResponseUtils {
    public static Map<String, Object> getExceptionResponse(final Throwable e) {
        Map<String, Object> result = new HashMap<>();
        result.put("status", "error");
        result.put("error", e.getClass());
        result.put("message", e.getMessage());
        return result;
    }
}
