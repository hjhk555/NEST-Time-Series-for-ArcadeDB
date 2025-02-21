package nju.hjh.arcadedb.timeseries.server.dto;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
@ToString
public class QueryDto {
    @SerializedName("target")
    private String targetType;

    @SerializedName("rid")
    private String vertexRID;

    @SerializedName("sql")
    private String sql;

    @SerializedName("id")
    private String id;

    @SerializedName("start")
    private Long start;

    @SerializedName("end")
    private Long end;

    @SerializedName("fields")
    private List<String> queryFields;
}
