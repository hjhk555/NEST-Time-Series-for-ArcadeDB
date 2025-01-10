package nju.hjh.arcadedb.timeseries.server.bo;

import lombok.Getter;
import lombok.ToString;
import nju.hjh.arcadedb.timeseries.UpdateStrategy;
import java.util.Map;

@Getter
@ToString
public class Metric {
    public String objectType;
    public String objectId;
    public String metricName;
    public UpdateStrategy strategy;
    public Map<Long, Object> dataPoints;
}
