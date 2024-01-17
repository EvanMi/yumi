package com.yumi.longhttppull;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

@SpringBootApplication
@ServletComponentScan(basePackages = "com.yumi.longhttppull.servlet")
public class LongHttpPullApplication {

	public static void main(String[] args) {
		SpringApplication.run(LongHttpPullApplication.class, args);
	}

}
