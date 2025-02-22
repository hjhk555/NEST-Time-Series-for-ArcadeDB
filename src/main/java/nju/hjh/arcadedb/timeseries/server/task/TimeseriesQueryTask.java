package nju.hjh.arcadedb.timeseries.server.task;

import lombok.Getter;
import nju.hjh.arcadedb.timeseries.server.bo.TimeseriesQuery;

import java.util.List;

@Getter
public class TimeseriesQueryTask implements DatabaseTask{
    List<TimeseriesQuery> queries;

    public TimeseriesQueryTask(List<TimeseriesQuery> queries) {
        this.queries = queries;
    }
}
