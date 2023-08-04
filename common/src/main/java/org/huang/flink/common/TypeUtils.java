package org.huang.flink.common;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.security.Key;

public class TypeUtils {
    public static <T, K> TypeInformation<Tuple2<T, K>> tuple2(Class<T> c1, Class<K> c2) {
        return TypeInformation.of(new TypeHint<Tuple2<T, K>>() {});
    }
}
