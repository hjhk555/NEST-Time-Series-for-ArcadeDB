package nju.hjh.arcadedb.timeseries.server.bo;

import lombok.Getter;

import java.util.List;

@Getter
public class TimeseriesInsertTask implements DatabaseTask{
    List<Metric> metrics;

    public TimeseriesInsertTask(List<Metric> metrics) {
        this.metrics = metrics;
    }
}
