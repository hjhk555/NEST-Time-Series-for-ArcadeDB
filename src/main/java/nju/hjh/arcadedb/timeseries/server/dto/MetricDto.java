package nju.hjh.arcadedb.timeseries.server.dto;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

@Getter
@Setter
@ToString
public class MetricDto {
    @SerializedName("id")
    private String objectId;

    @SerializedName("metric")
    private String metricName;

    @SerializedName("conflict")
    private String conflict;

    @SerializedName("points")
    private Map<String, Object> dataPoints;
}
