package com.chi.pattern.builder;

/**
 * @author chi
 * @Description: TODO
 * @date 2021/11/2 13:10
 * @Version 1.0
 */
public class BuilderDemo {
    public static void main(String[] args) {
        Person person = new Person.Builder("张三","男")
                .age("12")
                .money("1000000")
                .car("宝马")
                .build();
    }
}


