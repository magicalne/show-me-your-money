package io.magicalne.smym.dto;

import lombok.Data;

@Data
public class GetOrdersRequest {
    /**
     types	false	string	查询的订单类型组合，使用','分割		buy-market：市价买, sell-market：市价卖, buy-limit：限价买, sell-limit：限价卖, buy-ioc：IOC买单, sell-ioc：IOC卖单
     start-date	false	string	查询开始日期, 日期格式yyyy-mm-dd
     end-date	false	string	查询结束日期, 日期格式yyyy-mm-dd
     states	true	string	查询的订单状态组合，使用','分割		submitted 已提交, partial-filled 部分成交, partial-canceled 部分成交撤销, filled 完全成交, canceled 已撤销
     from	false	string	查询起始 ID
     direct	false	string	查询方向		prev 向前，next 向后
     size	false	string	查询记录大小
     */
    private String symbol; //btcusdt, bchbtc, rcneth ...
    private String states; //使用','分割		submitted 已提交, partial-filled 部分成交, partial-canceled 部分成交撤销, filled 完全成交, canceled 已撤销

    private String types;
    private String startDate; //日期格式yyyy-mm-dd
    private String endDate; //日期格式yyyy-mm-dd
    private String from; //order id
    private String direct; //prev, next
    private String size;
}
