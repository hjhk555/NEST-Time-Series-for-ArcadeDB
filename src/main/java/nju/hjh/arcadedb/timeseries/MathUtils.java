package nju.hjh.arcadedb.timeseries;

import java.util.List;

public class MathUtils {
    public static int bytesToWriteUnsignedNumber(long number) {
        int bytes = 1;
        number >>>= 7;
        while (number != 0) {
            bytes++;
            number >>>= 7;
        }
        return bytes;
    }

    public interface LongExtractor<T>{
        long getLong(T object);
    }

    /**
     * search target in given array and range, return the pos of former one
     * @param beginIdx inclusive begin of range
     * @param endIdx exclusive end of range
     * @param extractor method for extracting long value from object
     */
    public static <T> int longBinarySearchFormer(T[] array, int beginIdx, int endIdx, long target, LongExtractor<T> extractor){
        int low = beginIdx, high = endIdx-1;
        while (low <= high) {
            int mid = (low + high) >> 1;
            long midStartTime = extractor.getLong(array[mid]);
            if (midStartTime > target)
                high = mid - 1;
            else if (midStartTime < target)
                low = mid + 1;
            else {
                return mid;
            }
        }
        return high;
    }

    /**
     * search target in given array and range, return the pos of latter one
     * @param beginIdx inclusive begin of range
     * @param endIdx exclusive end of range
     * @param extractor method for extracting long value from object
     */
    public static <T> int longBinarySearchLatter(T[] array, int beginIdx, int endIdx, long target, LongExtractor<T> extractor){
        int low = beginIdx, high = endIdx-1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midStartTime = extractor.getLong(array[mid]);
            if (midStartTime > target)
                high = mid - 1;
            else if (midStartTime < target)
                low = mid + 1;
            else {
                return mid;
            }
        }
        return low;
    }
}
