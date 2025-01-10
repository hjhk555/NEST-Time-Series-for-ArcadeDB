package nju.hjh.arcadedb.timeseries.server.dto;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.ToString;

import java.util.Map;

@Getter
@ToString
public class MetricDto {
    @SerializedName("type")
    public String objectType;

    @SerializedName("id")
    public String objectId;

    @SerializedName("metric")
    public String metricName;

    @SerializedName("conflict")
    public String conflict;

    @SerializedName("points")
    public Map<String, Object> dataPoints;
}
