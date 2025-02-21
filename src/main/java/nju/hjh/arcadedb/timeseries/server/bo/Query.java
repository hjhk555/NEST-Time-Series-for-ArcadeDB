package nju.hjh.arcadedb.timeseries.server.bo;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

@Getter
@ToString
public class Query {
    public String targetType;
    public String id;
    public String vertexRID;
    public String sql;
    public Long start;
    public Long end;
    public List<String> queryFields;
}
