package com.yumi.eventstream.controller;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.LocalDateTime;
import java.util.Enumeration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Controller
public class SseController {

    private static final ExecutorService sseMvcExecutor = Executors.newFixedThreadPool(8);

    @GetMapping("/stream-sse-mvc")
    @CrossOrigin
    public SseEmitter streamSseMvc(@RequestHeader(name = "last-event-id", required = false) String lastEventId,
                                   @RequestHeader("accept") String accept) throws Exception{
        System.out.println("accept: " + accept + "   last-event-id: " + lastEventId);
        SseEmitter sseEmitter = new SseEmitter(0L);

        final Integer continueI = lastEventId == null ? 0 : Integer.parseInt(
                lastEventId.substring(lastEventId.lastIndexOf(":") + 2)
        );
        sseMvcExecutor.execute(() -> {
            try {
                for (int i = continueI; true; i++) {
                    SseEmitter.SseEventBuilder eb = null;
                    if (i % 10 == 0) {
                        eb = SseEmitter.event()
                                .id("id: " + i)
                                .reconnectTime(10000)
                                .name("yumi")
                                .data("Yumi-MSG" + LocalDateTime.now());
                    } else {
                        eb = SseEmitter.event()
                                .reconnectTime(10000)
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
