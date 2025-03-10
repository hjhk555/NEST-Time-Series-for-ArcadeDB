package nju.hjh.arcadedb.timeseries.server.controller;

import nju.hjh.arcadedb.timeseries.server.bo.Metric;
import nju.hjh.arcadedb.timeseries.server.bo.TimeseriesQuery;
import nju.hjh.arcadedb.timeseries.server.task.TimeseriesInsertTask;
import nju.hjh.arcadedb.timeseries.server.dto.MetricDto;
import nju.hjh.arcadedb.timeseries.server.data.NestDatabaseManager;
import nju.hjh.arcadedb.timeseries.server.dto.TimeseriesQueryDto;
import nju.hjh.arcadedb.timeseries.server.task.TimeseriesQueryTask;
import nju.hjh.arcadedb.timeseries.server.utils.GsonUtils;
import nju.hjh.arcadedb.timeseries.server.utils.DtoUtils;
import nju.hjh.arcadedb.timeseries.server.utils.ResponseUtils;
import org.springframework.web.bind.annotation.*;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/timeseries")
public class TimeseriesController {
    private static final Type typeMetricList = new TypeToken<List<MetricDto>>(){}.getType();
    private static final Type typeQueryList = new TypeToken<List<TimeseriesQueryDto>>(){}.getType();

    @PostMapping("/insert")
    public Map<String, Object> insertTimeseries(@RequestParam("database") String dbName, @RequestBody String jsonMetricDtoList) {
        try {
            List<MetricDto> metricDtos = GsonUtils.fromJson(jsonMetricDtoList, typeMetricList);
            List<Metric> metrics = new ArrayList<>();
            for (MetricDto metricDto : metricDtos) {
                if (DtoUtils.validateMetricDto(metricDto)) metrics.add(DtoUtils.convertMetricDto2Bo(metricDto));
            }
            if (metrics.isEmpty()) {
                Map<String, Object> result = new HashMap<>();
                result.put("status", "ok");
                return result;
            }
            CompletableFuture<Map<String, Object>> resultFuture = NestDatabaseManager.getDatabaseManager(dbName).submitTask(new TimeseriesInsertTask(metrics));
            return resultFuture.get();
        } catch (Exception e){
            return ResponseUtils.getExceptionResponse(e);
        }
    }

    @GetMapping("/query")
    public Map<String, Object> queryTimeseries(@RequestParam("database") String dbName, @RequestBody String jsonQueryDtoList) {
        try{
            List<TimeseriesQueryDto> timeseriesQueryDtos = GsonUtils.fromJson(jsonQueryDtoList, typeQueryList);
            List<TimeseriesQuery> queries = new ArrayList<>();
            for (TimeseriesQueryDto timeseriesQueryDto : timeseriesQueryDtos) {
                if (DtoUtils.validateTimeseriesQueryDto(timeseriesQueryDto)) queries.add(DtoUtils.convertTimeseriesQueryDto2Bo(timeseriesQueryDto));
            }
            if (queries.isEmpty()) {
                Map<String, Object> result = new HashMap<>();
                result.put("status", "ok");
                return result;
            }
            CompletableFuture<Map<String, Object>> resultFuture = NestDatabaseManager.getDatabaseManager(dbName).submitTask(new TimeseriesQueryTask(queries));
            return resultFuture.get();
        } catch (Exception e){
            return ResponseUtils.getExceptionResponse(e);
        }
    }
}
