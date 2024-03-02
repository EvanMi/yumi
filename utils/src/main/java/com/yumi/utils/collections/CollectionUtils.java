package com.yumi.utils.collections;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class CollectionUtils {
    private CollectionUtils() {
    }

    public static <T> void removeMatched(List<T> list, Function<T, Boolean> matcher) {
        Iterator<T> iterator = list.iterator();
        while (iterator.hasNext()) {
            if (matcher.apply(iterator.next())) {
                iterator.remove();
            }
        }
    }


}
