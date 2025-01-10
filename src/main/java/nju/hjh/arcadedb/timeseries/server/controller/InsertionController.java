package nju.hjh.arcadedb.timeseries.server.controller;

import com.arcadedb.graph.MutableVertex;
import nju.hjh.arcadedb.timeseries.NestEngine;
import nju.hjh.arcadedb.timeseries.server.bo.Metric;
import nju.hjh.arcadedb.timeseries.server.dto.MetricDto;
import nju.hjh.arcadedb.timeseries.server.utils.DatabaseUtils;
import nju.hjh.arcadedb.timeseries.server.utils.GsonUtils;
import nju.hjh.arcadedb.timeseries.server.utils.MetricUtils;
import nju.hjh.arcadedb.timeseries.server.utils.ResponseUtils;
import org.springframework.web.bind.annotation.*;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/insert")
public class InsertionController {
    private static final Type typeMetricList = new TypeToken<List<MetricDto>>(){}.getType();

    @PostMapping("/ts")
    public Map<String, Object> insertTimeseries(@RequestParam("database") String dbName, @RequestBody String jsonMetricDtoList) {
        try {
            List<MetricDto> metricDtos = GsonUtils.fromJson(jsonMetricDtoList, typeMetricList);
            List<Metric> metrics = new ArrayList<>();
            for (MetricDto metricDto : metricDtos) {
                if (MetricUtils.validateMetricDto(metricDto)) metrics.add(MetricUtils.convertDto2Bo(metricDto));
            }
            NestEngine engine = DatabaseUtils.getNestDatabase(dbName);
            engine.begin();
            try{
                for (Metric metric : metrics) {
                    MutableVertex vtxObject = DatabaseUtils.getOrCreateSingleVertex(engine.getDatabase(), metric.getObjectType(), metric.getObjectId()).modify();
                    for (Map.Entry<Long, Object> datapoint: metric.getDataPoints().entrySet()){
                        engine.insertDataPoint(vtxObject, metric.getMetricName(), datapoint.getKey(), datapoint.getValue(), metric.getStrategy());
                    }
                }
                engine.commit();
                Map<String, Object> result = new HashMap<>();
                result.put("status", "ok");
                return result;
            } catch (Exception e){
                engine.rollback();
                throw e;
            }
        } catch (Exception e){
            return ResponseUtils.getExceptionResponse(e);
        }
    }
}
