package com.streaming.kafka;

import org.springframework.aop.MethodBeforeAdvice;

import java.lang.reflect.Method;

public class WelcomeAdvice implements MethodBeforeAdvice {
    @Override
    public void before(Method method, Object[] objects, Object o) throws Throwable {
        System.out.println("I have no clue!!! Lets see");
    }
}
