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
     * return the position of the largest value which is not larger than given one.
     * for example, given list [1,3,5] with target 8, it will return 2.
     * with target 3, it will return 1.
     * with target 0, it will return -1.
     * @param list the list for search
     * @param target the target value
     * @return the position of the largest value which is not larger than target, -1 if all value is larger than target
     */
    public static int longBinarySearchFormer(List<Long> list, long target){
        int low = 0, high = list.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midStartTime = list.get(mid);
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
     * add extractor to method {@link #longBinarySearchFormer(List, long) longBinarySearchFormer}
     * @param extractor method for extracting long value from object
     */
    public static <T> int longBinarySearchFormer(List<T> list, long target, LongExtractor<T> extractor){
        int low = 0, high = list.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midStartTime = extractor.getLong(list.get(mid));
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
     * return the position of the smallest value which is not smaller than given one.
     * for example, given list [1,3,5] with target 2, it will return 1.
     * with target 5, it will return 2.
     * with target 6, it will return 3.
     * @param list the list for search
     * @param target the target value
     * @return the position of the smallest value which is not smaller than target, list size if all value is smaller than target
     */
    public static int longBinarySearchLatter(List<Long> list, long target){
        int low = 0, high = list.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midStartTime = list.get(mid);
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

    /**
     * add extractor to method {@link #longBinarySearchLatter(List, long) longBinarySearchLatter}
     * @param extractor method for extracting long value from object
     */
    public static <T> int longBinarySearchLatter(List<T> list, long target, LongExtractor<T> extractor){
        int low = 0, high = list.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midStartTime = extractor.getLong(list.get(mid));
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

    /**
     * return the position of the given value.
     * for example, given list [1,3,5] with target 3, it will return 1.
     * with target 2, it will return -1.
     * @param list the list for search
     * @param target the target value
     * @return the position of the target, -1 if target doesn't exist.
     */
    public static int longBinarySearch(List<Long> list, long target){
        int low = 0, high = list.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midStartTime = list.get(mid);
            if (midStartTime > target)
                high = mid - 1;
            else if (midStartTime < target)
                low = mid + 1;
            else {
                return mid;
            }
        }
        return -1;
    }

    /**
     * add extractor to method {@link #longBinarySearch(List, long) longBinarySearch}
     * @param extractor method for extracting long value from object
     */
    public static <T> int longBinarySearch(List<T> list, long target, LongExtractor<T> extractor){
        int low = 0, high = list.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midStartTime = extractor.getLong(list.get(mid));
            if (midStartTime > target)
                high = mid - 1;
            else if (midStartTime < target)
                low = mid + 1;
            else {
                return mid;
            }
        }
        return -1;
    }
}
