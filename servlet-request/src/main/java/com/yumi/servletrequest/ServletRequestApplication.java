package com.yumi.servletrequest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

@SpringBootApplication
@ServletComponentScan(basePackages = "com.yumi.servletrequest.servlet")
public class ServletRequestApplication {

	public static void main(String[] args) {
		SpringApplication.run(ServletRequestApplication.class, args);
	}

}
