package nju.hjh.arcadedb.timeseries.server.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.HashMap;
import java.util.Map;

@RequestMapping("/query")
public class QueryController {
    @GetMapping("/ts")
    public Map<String, Object> queryTimeseries(@RequestParam("database") String dbName, @RequestBody String jsonQueryDto) {
        return new HashMap<>();
    }
}
