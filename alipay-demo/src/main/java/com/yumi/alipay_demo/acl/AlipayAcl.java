package com.yumi.alipay_demo.acl;

import com.alibaba.fastjson.JSON;
import com.alipay.api.AlipayApiException;
import com.alipay.api.AlipayClient;
import com.alipay.api.AlipayConstants;
import com.alipay.api.DefaultAlipayClient;
import com.alipay.api.internal.util.AlipaySignature;
import com.alipay.api.request.AlipayTradePagePayRequest;
import com.yumi.alipay_demo.config.PropertiesConfig;
import com.yumi.alipay_demo.dto.AlipayBean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class AlipayAcl {
    public String pay(AlipayBean alipayBean) throws AlipayApiException {
        // 1、获得初始化的AlipayClient
        String serverUrl = PropertiesConfig.GATEWAY_URL;
        String appId = PropertiesConfig.APP_ID;
        String privateKey = PropertiesConfig.PRIVARY_KEY;
        String format = "json";
        String charset = PropertiesConfig.CHARSET;
        String alipayPublicKey = PropertiesConfig.PUBLIC_KEY;
        String signType = PropertiesConfig.SIGN_TYPE;
        String returnUrl = PropertiesConfig.RETURN_URL;
        String notifyUrl = PropertiesConfig.NOTIFY_URL;
        AlipayClient alipayClient = new DefaultAlipayClient(serverUrl, appId, privateKey, format,
                charset, alipayPublicKey, signType);
        // 2、设置请求参数
        AlipayTradePagePayRequest alipayRequest = new AlipayTradePagePayRequest();
        // 页面跳转同步通知页面路径
        alipayRequest.setReturnUrl(returnUrl);
        // 服务器异步通知页面路径
        alipayRequest.setNotifyUrl(notifyUrl);
        // 封装参数
        alipayRequest.setBizContent(JSON.toJSONString(alipayBean));
        // 3、请求支付宝进行付款，并获取支付结果
        //通过以下代码进行提交请求会默认返回请求url字符串（返回url，打开跳转支付宝支付页面）
        String result = alipayClient.pageExecute(alipayRequest, "GET").getBody();
        log.info("result: {}", result);
        // 返回付款信息
        return result;
    }


    public String tradeNotify(Map<String, String> params) {
        String result = "failure";
        try {
            //异步通知验签
            boolean signVerified = AlipaySignature.rsaCheckV1(params,
                    PropertiesConfig.PUBLIC_KEY,
                    AlipayConstants.CHARSET_UTF8,
                    AlipayConstants.SIGN_TYPE_RSA2);
            if (!signVerified) {
                log.error("支付成功,异步通知验签失败!");
                return result;
            }
            log.info("支付成功,异步通知验签成功!");
            //1.验证out_trade_no 是否为商家系统中创建的订单号
            String outTradeNo = params.get("out_trade_no");
            //2.判断 total_amount 是否确实为该订单的实际金额
            String totalAmount = params.get("total_amount");
            //3.校验通知中的 seller_id是否为 out_trade_no 这笔单据的对应的操作方
            String sellerId = params.get("seller_id");
            if (!sellerId.equals(PropertiesConfig.SELLER_ID)) {
                log.error("商家PID校验失败");
                return result;
            }
            //4.验证 app_id 是否为该商家本身
            String appId = params.get("app_id");
            if (!appId.equals(PropertiesConfig.APP_ID)) {
                log.error("app_id校验失败");
                return result;
            }
            //在支付宝的业务通知中，只有交易通知状态为 TRADE_SUCCESS 或 TRADE_FINISHED 时，支付宝才会认定为买家付款成功
            String tradeStatus = params.get("trade_status");
            if (!"TRADE_SUCCESS".equals(tradeStatus) && !"TRADE_FINISHED".equals(tradeStatus)) {
                log.error("支付未成功");
                return result;
            }

            //TODO 处理自身业务

            log.info("params: {}", params);
            result = "success";
        } catch (AlipayApiException e) {
            e.printStackTrace();
        }

        return result;
    }
}
