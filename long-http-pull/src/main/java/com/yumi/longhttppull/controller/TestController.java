package com.yumi.longhttppull.controller;


import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class TestController {

    @RequestMapping("/testPage")
    public String gg() {
        return "test";

    }
}