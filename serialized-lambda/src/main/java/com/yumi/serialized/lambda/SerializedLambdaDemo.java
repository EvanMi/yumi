package com.yumi.serialized.lambda;

import com.baomidou.mybatisplus.core.toolkit.support.SFunction;
import com.baomidou.mybatisplus.core.toolkit.support.SerializedLambda;
import com.yumi.serialized.model.Person;

public class SerializedLambdaDemo {

    public static void main(String[] args) {
        SFunction<Person, ?> sf = Person::getName;
        SerializedLambda extract = SerializedLambda.extract(sf);
        System.out.println(extract.getImplMethodName());
    }
}
