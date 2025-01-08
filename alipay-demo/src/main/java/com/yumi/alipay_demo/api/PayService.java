package com.yumi.alipay_demo.api;

import com.yumi.alipay_demo.acl.AlipayAcl;
import com.yumi.alipay_demo.dto.AlipayBean;
import com.yumi.alipay_demo.dto.PayCommand;
import jakarta.annotation.Resource;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.math.RoundingMode;

@RestController
@RequestMapping("/pay")
@Slf4j
public class PayService {
    @Resource
    private AlipayAcl acl;
    @PostMapping("/url")
    @SneakyThrows
    public String getPayUrl(@RequestBody PayCommand payCommand){
        AlipayBean alipayBean = new AlipayBean();
        alipayBean.setOut_trade_no(payCommand.getOrderId().toString());
        alipayBean.setSubject("红包订单");
        alipayBean.setTotal_amount(new BigDecimal(payCommand.getMoney().toString())
                .divide(BigDecimal.valueOf(1000), 2, RoundingMode.HALF_DOWN)
                .toString());
        String ids= "courseId" +","+ "yumi" + "," + payCommand.getOrderId();
        alipayBean.setBody(ids);
        return acl.pay(alipayBean);
    }
}
