package nju.hjh.arcadedb.timeseries.server.controller;

import nju.hjh.arcadedb.timeseries.exception.DatabaseException;
import nju.hjh.arcadedb.timeseries.server.data.NestDatabaseManager;
import nju.hjh.arcadedb.timeseries.server.utils.ResponseUtils;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/database")
public class DatabaseController {
    @PostMapping()
    public Map<String, Object> create(@RequestParam("name") String dbName) {
        Map<String, Object> result = new HashMap<>();
        try {
            NestDatabaseManager.getDatabaseManager(dbName).createDatabase();
            result.put("status", "ok");
            return result;
        } catch (DatabaseException e) {
            return ResponseUtils.getExceptionResponse(e);
        }
    }

    @GetMapping()
    public Map<String, Object> exists(@RequestParam("name") String dbName) {
        Map<String, Object> result = new HashMap<>();
        result.put("status", "ok");
        result.put("name", dbName);
        result.put("exists", String.valueOf(NestDatabaseManager.getDatabaseManager(dbName).isDatabaseExists()));
        return result;
    }

    @DeleteMapping()
    public Map<String, Object> delete(@RequestParam("name") String dbName) {
        Map<String, Object> result = new HashMap<>();
        try {
            NestDatabaseManager.getDatabaseManager(dbName).drop();
            result.put("status", "ok");
            return result;
        } catch (DatabaseException e) {
            return ResponseUtils.getExceptionResponse(e);
        }
    }
}
