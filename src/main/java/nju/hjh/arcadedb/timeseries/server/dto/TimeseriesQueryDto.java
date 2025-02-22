package nju.hjh.arcadedb.timeseries.server.dto;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
@ToString
public class TimeseriesQueryDto {
    public static final int MAX_LIMIT = 1000;
    @SerializedName("id")
    private String objectId;

    @SerializedName("rid")
    private String vertexRID;

    @SerializedName("sql")
    private String sql;

    @SerializedName("start")
    private Long start;

    @SerializedName("end")
    private Long end;

    @SerializedName("limit")
    private Integer limit;

    @SerializedName("fields")
    private List<String> queryFields;
}
