package com.yumi.dbs;

import com.yumi.dbs.mapper.BillionaireMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class BaseDbTests {

	@Autowired
	BillionaireMapper mapper;

	@Test
	void contextLoads() {
		mapper.selectList(null).forEach(System.out::println);
	}

}
