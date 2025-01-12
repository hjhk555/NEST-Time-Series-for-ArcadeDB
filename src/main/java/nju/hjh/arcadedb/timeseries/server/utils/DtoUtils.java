package nju.hjh.arcadedb.timeseries.server.utils;

import nju.hjh.arcadedb.timeseries.UpdateStrategy;
import nju.hjh.arcadedb.timeseries.exception.MessageParsingException;
import nju.hjh.arcadedb.timeseries.exception.MissingFieldException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.server.bo.Metric;
import nju.hjh.arcadedb.timeseries.server.dto.MetricDto;
import nju.hjh.arcadedb.timeseries.server.dto.QueryDto;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class DtoUtils {
    public static boolean validateMetricDto(final MetricDto metricDto) throws TimeseriesException{
        if (MapUtils.isEmpty(metricDto.getDataPoints())) return false;
        if (StringUtils.isEmpty(metricDto.getMetricName())) throw new MissingFieldException("metric");
        String id = metricDto.getObjectId();
        if (StringUtils.isEmpty(id)) throw new MissingFieldException("id");
        if (id.indexOf(':') == -1) throw new MessageParsingException("id must be in form of {type}:{id}");
        return true;
    }

    public static Metric convertMetricDto2Bo(MetricDto dto) throws TimeseriesException {
        Metric metric = new Metric();
        String id = dto.getObjectId();
        int sepIndex = id.indexOf(':');
        metric.objectType = id.substring(0, sepIndex);
        metric.objectId = id.substring(sepIndex + 1);
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

    public static boolean validateQueryDto(final QueryDto dto) throws TimeseriesException {
        
    }
}
