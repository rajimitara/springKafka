package com.streaming.kafka;

import org.springframework.aop.support.DelegatingIntroductionInterceptor;

public class Rollback extends DelegatingIntroductionInterceptor implements KafkaIntro {
    public void intro() {
        //super.intro();
    }
}
