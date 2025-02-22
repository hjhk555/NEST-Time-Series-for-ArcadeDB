package nju.hjh.arcadedb.timeseries.server.task;

import lombok.Getter;
import nju.hjh.arcadedb.timeseries.server.bo.Metric;

import java.util.List;

@Getter
public class TimeseriesInsertTask implements DatabaseTask{
    List<Metric> metrics;

    public TimeseriesInsertTask(List<Metric> metrics) {
        this.metrics = metrics;
    }
}
