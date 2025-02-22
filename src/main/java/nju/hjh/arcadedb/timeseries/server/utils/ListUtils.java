package nju.hjh.arcadedb.timeseries.server.utils;

import java.util.Collection;

public class ListUtils {
    public static boolean isEmpty(final Collection<?> c) {
        return c == null || c.isEmpty();
    }
}
