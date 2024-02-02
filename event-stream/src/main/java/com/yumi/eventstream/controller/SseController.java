package com.yumi.eventstream.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Controller
public class SseController {

    private static final ExecutorService sseMvcExecutor = Executors.newFixedThreadPool(8);

    @GetMapping("/stream-sse-mvc")
    @CrossOrigin
    public SseEmitter streamSseMvc() throws Exception{
        SseEmitter sseEmitter = new SseEmitter(0L);
        sseMvcExecutor.execute(() -> {
            try {
                for (int i = 0; true; i++) {
                    SseEmitter.SseEventBuilder eb = null;
                    if (i % 10 == 0) {
                        eb = SseEmitter.event()
                                .id("id: " + i)
                                .name("yumi")
                                .data("Yumi-MSG" + LocalDateTime.now());
                    } else {
                        eb = SseEmitter.event()
                                .data("Test-MSG" + LocalDateTime.now());
                    }
                    sseEmitter.send(eb);
                    Thread.sleep(1000);
                }
                //sseEmitter.complete();
            } catch (Exception e) {
                sseEmitter.completeWithError(e);
            }
        });
        //sseEmitter.send(SseEmitter.event().data("am"));
        return sseEmitter;
    }
}
