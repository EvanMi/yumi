package com.yumi.serialized.util;

import com.baomidou.mybatisplus.core.toolkit.support.SerializedLambda;

import java.io.Serializable;
import java.lang.reflect.Field;

public class YumiSerializedLambda {
    public static SerializedLambda extract(Serializable s) throws Exception{
        Class<? extends Serializable> aClass = s.getClass();
        Field[] declaredFields = aClass.getDeclaredFields();
        Object fieldVal = null;
        Field declaredField = null;
        if (declaredFields.length > 0) {
            declaredField = declaredFields[0];
            declaredField.setAccessible(true);
            fieldVal = declaredField.get(s);
            declaredField.set(s, null);
        }
        SerializedLambda extract = SerializedLambda.extract(s);
        if (null != declaredField && null != fieldVal) {
            declaredField.set(s, fieldVal);
        }
        return extract;
    }
}
