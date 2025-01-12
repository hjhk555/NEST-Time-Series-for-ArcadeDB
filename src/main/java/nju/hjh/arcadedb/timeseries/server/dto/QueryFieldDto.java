package nju.hjh.arcadedb.timeseries.server.dto;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class QueryFieldDto {
    @SerializedName("metric")
    private String metric;

    @SerializedName("type")
    private String type;
}
