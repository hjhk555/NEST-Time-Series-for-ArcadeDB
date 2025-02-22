package nju.hjh.arcadedb.timeseries.server.utils;

import com.arcadedb.database.RID;
import nju.hjh.arcadedb.timeseries.UpdateStrategy;
import nju.hjh.arcadedb.timeseries.exception.MessageParsingException;
import nju.hjh.arcadedb.timeseries.exception.MissingFieldException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.server.bo.Metric;
import nju.hjh.arcadedb.timeseries.server.bo.TimeseriesQuery;
import nju.hjh.arcadedb.timeseries.server.dto.MetricDto;
import nju.hjh.arcadedb.timeseries.server.dto.TimeseriesQueryDto;

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

    public static Metric convertMetricDto2Bo(final MetricDto dto) throws TimeseriesException {
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

    public static boolean validateTimeseriesQueryDto(final TimeseriesQueryDto queryDto) throws TimeseriesException {
        if (ListUtils.isEmpty(queryDto.getQueryFields())) return false;
        if (StringUtils.isEmpty(queryDto.getObjectId()) &&
            StringUtils.isEmpty(queryDto.getVertexRID()) &&
            StringUtils.isEmpty(queryDto.getSql())){
        }
        if (!StringUtils.isEmpty(queryDto.getObjectId())) {
            if (queryDto.getObjectId().indexOf(':') == -1) throw new MessageParsingException("id must be in form of {type}:{id}");
        } else if (!StringUtils.isEmpty(queryDto.getVertexRID())) {
            int sepIndex = queryDto.getVertexRID().indexOf(':');
            if (sepIndex == -1) throw new MessageParsingException("rid must be in form of {bucket}:{offset}");
            try{
                Integer.parseInt(queryDto.getVertexRID().substring(0, sepIndex));
                Long.parseLong(queryDto.getVertexRID().substring(sepIndex + 1));
            }catch (NumberFormatException e){
                throw new MessageParsingException("rid bucket or offset is not a number");
            }
        } else if (StringUtils.isEmpty(queryDto.getSql())) {
            throw new MessageParsingException("one of id/rid/sql is required");
        }
        if (queryDto.getLimit() != null && queryDto.getLimit() > TimeseriesQueryDto.MAX_LIMIT){
            throw new MessageParsingException("limit is greater than max limit "+TimeseriesQueryDto.MAX_LIMIT);
        }

        return true;
    }

    public static TimeseriesQuery convertTimeseriesQueryDto2Bo(final TimeseriesQueryDto queryDto) throws TimeseriesException {
        TimeseriesQuery query = new TimeseriesQuery();
        if (!StringUtils.isEmpty(queryDto.getObjectId())){
            int sepIndex = queryDto.getObjectId().indexOf(':');
            query.objectType = queryDto.getObjectId().substring(0, sepIndex);
            query.objectId = queryDto.getObjectId().substring(sepIndex+1);
        }else if (!StringUtils.isEmpty(queryDto.getVertexRID())){
            int sepIndex = queryDto.getVertexRID().indexOf(':');
            query.ridBucket = Integer.parseInt(queryDto.getVertexRID().substring(0, sepIndex));
            query.ridOffset = Long.parseLong(queryDto.getVertexRID().substring(sepIndex+1));
        }else {
            query.sql = queryDto.getSql();
        }
        query.start = queryDto.getStart() == null ? 0 : queryDto.getStart();
        query.end = queryDto.getEnd() == null ? Long.MAX_VALUE : queryDto.getEnd();
        query.limit = queryDto.getLimit() == null ? TimeseriesQueryDto.MAX_LIMIT : queryDto.getLimit();
        query.queryFields = queryDto.getQueryFields();
        return query;
    }
}
