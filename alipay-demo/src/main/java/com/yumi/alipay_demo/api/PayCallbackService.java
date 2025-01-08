package com.yumi.alipay_demo.api;

import com.alibaba.fastjson.JSON;
import com.alipay.api.AlipayApiException;
import com.alipay.api.AlipayConstants;
import com.alipay.api.internal.util.AlipaySignature;
import com.yumi.alipay_demo.acl.AlipayAcl;
import com.yumi.alipay_demo.config.PropertiesConfig;
import jakarta.annotation.Resource;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.view.RedirectView;

import java.util.Map;

@Controller
@RequestMapping("/callback")
@Slf4j
public class PayCallbackService {
    @Resource
    private AlipayAcl acl;

    @SneakyThrows
    @PostMapping("/tradeNotify")
    public String tradeNotify(@RequestParam Map<String, String> params)  {
        log.info("支付通知,正在执行,通知参数:{}", JSON.toJSONString(params));
        return acl.tradeNotify(params);
    }


    @GetMapping("/tradeCallback")
    public RedirectView tradeCallback(@RequestParam Map<String, String> params) throws AlipayApiException {
        log.info("支付回调,正在执行,通知参数:{}", JSON.toJSONString(params));
        boolean signVerified = AlipaySignature.rsaCheckV1(params,
                PropertiesConfig.PUBLIC_KEY,
                AlipayConstants.CHARSET_UTF8,
                AlipayConstants.SIGN_TYPE_RSA2);
        log.info("signVerified: {}", signVerified);
        return new RedirectView("https://www.baidu.com");
    }
}
