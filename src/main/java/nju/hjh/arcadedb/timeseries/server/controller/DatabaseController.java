package nju.hjh.arcadedb.timeseries.server.controller;

import nju.hjh.arcadedb.timeseries.exception.DatabaseException;
import nju.hjh.arcadedb.timeseries.server.utils.DatabaseUtils;
import nju.hjh.arcadedb.timeseries.server.utils.ResponseUtils;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/db")
public class DatabaseController {
    @PostMapping("/create")
    public Map<String, Object> create(@RequestParam("name") String dbName) {
        Map<String, Object> result = new HashMap<>();
        try {
            DatabaseUtils.createNestDatabase(dbName);
            result.put("status", "ok");
            return result;
        } catch (DatabaseException e) {
            return ResponseUtils.getExceptionResponse(e);
        }
    }

    @GetMapping("/exists")
    public Map<String, Object> exists(@RequestParam("name") String dbName) {
        Map<String, Object> result = new HashMap<>();
        result.put("status", "ok");
        result.put("name", dbName);
        result.put("exists", String.valueOf(DatabaseUtils.checkDatabaseExists(dbName)));
        return result;
    }

    @DeleteMapping("/delete")
    public Map<String, Object> delete(@RequestParam("name") String dbName) {
        Map<String, Object> result = new HashMap<>();
        try {
            DatabaseUtils.dropDatabase(dbName);
            result.put("status", "ok");
            return result;
        } catch (DatabaseException e) {
            return ResponseUtils.getExceptionResponse(e);
        }
    }
}
