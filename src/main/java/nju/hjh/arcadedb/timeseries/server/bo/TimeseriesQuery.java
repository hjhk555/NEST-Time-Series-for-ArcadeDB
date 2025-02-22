package nju.hjh.arcadedb.timeseries.server.bo;

import com.arcadedb.database.RID;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

@Getter
@ToString
public class TimeseriesQuery {
    public String objectType;
    public String objectId;
    public Integer ridBucket;
    public Long ridOffset;
    public String sql;
    public Long start;
    public Long end;
    public Integer limit;
    public List<String> queryFields;
}
