package com.yumi.dbs;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.yumi.dbs.mapper")
public class DbsApplication {

	public static void main(String[] args) {
		SpringApplication.run(DbsApplication.class, args);
	}

}
