package com.yumi.serialized.lambda;

import com.baomidou.mybatisplus.core.toolkit.support.SFunction;
import com.baomidou.mybatisplus.core.toolkit.support.SerializedLambda;
import com.yumi.serialized.model.YumiService;
import com.yumi.serialized.util.YumiSerializedLambda;

import java.io.Serializable;
import java.util.function.BiFunction;

public class SolveProblemDemo {



    public static void main(String[] args) throws Exception{
        System.out.println(way1());
        System.out.println(way2());
    }



    public static String way1() {
        SBifunction<YumiService, String, String> sbf = YumiService::copyName;
        SerializedLambda extract = SerializedLambda.extract(sbf);
        return extract.getImplMethodName();
    }

    interface SBifunction<T, U, R> extends BiFunction<T, U, R>, Serializable {

    }



    public static String way2() throws Exception{
        SFunction<String, String> sf = new YumiService()::copyName;
        SerializedLambda extract = YumiSerializedLambda.extract(sf);
        return extract.getImplMethodName();
    }
}
