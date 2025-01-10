package nju.hjh.arcadedb.timeseries.server.utils;

import nju.hjh.arcadedb.timeseries.UpdateStrategy;
import nju.hjh.arcadedb.timeseries.exception.MissingFieldException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.server.bo.Metric;
import nju.hjh.arcadedb.timeseries.server.dto.MetricDto;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class MetricUtils {
    public static boolean validateMetricDto(final MetricDto metricDto) throws TimeseriesException{
        if (MapUtils.isEmpty(metricDto.dataPoints)) return false;
        if (StringUtils.isEmpty(metricDto.objectId)) throw new MissingFieldException("id");
        if (StringUtils.isEmpty(metricDto.metricName)) throw new MissingFieldException("metric");
        if (StringUtils.isEmpty(metricDto.objectType)) throw new MissingFieldException("type");
        return true;
    }

    public static Metric convertDto2Bo(MetricDto dto) throws TimeseriesException {
        Metric metric = new Metric();
        metric.objectType = dto.getObjectType();
        metric.objectId = dto.getObjectId();
        metric.metricName = dto.getMetricName();
        metric.strategy = StringUtils.isEmpty(dto.getConflict())? UpdateStrategy.IGNORE: UpdateStrategy.parse(dto.getConflict());
        if (dto.getDataPoints() == null) {
            metric.dataPoints = new HashMap<>();
        } else {
            metric.dataPoints = dto.getDataPoints().entrySet().stream()
                    .collect(Collectors.toMap(entry -> Long.valueOf(entry.getKey()), Map.Entry::getValue));
        }
        return metric;
    }
}
