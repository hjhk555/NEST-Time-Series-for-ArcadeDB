package nju.hjh.arcadedb.timeseries.server.bo;

import java.util.HashMap;
import java.util.Map;

public class TimeseriesQueryResult {
    public String id;
    public String rid;
    public Map<String, Object> timeseries = new HashMap<>();
}
