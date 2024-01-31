package com.yumi.serialized.lambda;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.yumi.serialized.model.Person;

public class MyBatisPlusDemo {
    public static void main(String[] args) {
        LambdaQueryWrapper<Person> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.eq(Person::getName, "yumi");

        /*
        *
        * select * from person where name = 'yumi';
        *
        * */


        //com.baomidou.mybatisplus.core.conditions.AbstractLambdaWrapper.columnsToString
    }
}
