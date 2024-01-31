package com.yumi.serialized.lambda;

import com.baomidou.mybatisplus.core.toolkit.support.SFunction;
import com.baomidou.mybatisplus.core.toolkit.support.SerializedLambda;
import com.yumi.serialized.model.YumiService;

public class YumiServiceDemo {

    public static void main(String[] args) {
        System.out.println(extractGetName());
        System.out.println(extractCopyName());
    }

    public static String extractGetName() {
        SFunction<YumiService, ?> sf = YumiService::getName;
        SerializedLambda extract = SerializedLambda.extract(sf);
        return extract.getImplMethodName();
    }

    public static String extractCopyName() {
        YumiService yumiService = new YumiService();
        SFunction<String, ?> sf = yumiService::copyName;
        SerializedLambda extract = SerializedLambda.extract(sf);
        return extract.getImplMethodName();
    }
}
