package io.magicalne.smym.strategy.integration;

import io.magicalne.smym.dto.*;
import io.magicalne.smym.exception.CancelOrderException;
import io.magicalne.smym.exception.OrderPlaceException;
import io.magicalne.smym.exchanges.HuobiProRest;
import io.magicalne.smym.strategy.HuobiTriangleArbitrage;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TriangleArbitrageTest {

    private HuobiProRest client;
    @Before
    public void setup() {
        this.client = new HuobiProRest();
    }

    @Test
    public void test() {
        List<Symbol> res = client.getSymbols();
        for (Symbol s : res) {
            if ("iotausdt".equals(s.getSymbol()))
            System.out.println(s);
        }
    }

    @Test
    public void test1() {
        HuobiProRest client = new HuobiProRest();
        BalanceResponse balance = client.balance("2672827");
        System.out.println(balance);
    }

    @Test
    public void test2() {
        HuobiProRest client = new HuobiProRest();
        OrderPlaceRequest req = new OrderPlaceRequest();
        req.setAccountId("2672827");
        req.setAmount("0.0123");
        req.setSymbol("eosusdt");
        req.setType(OrderType.SELL_LIMIT.getType());
        req.setPrice("5.6584");
        System.out.println(req);
        OrderPlaceResponse res = client.orderPlace(req);
        System.out.println(res);
    }

    @Test
    public void test3() {
        HuobiProRest client = new HuobiProRest();
//        SubmitcancelResponse submitcancel = client.submitcancel("9746118045");
//        System.out.println(submitcancel);
        OrdersDetailResponse res = client.ordersDetail("9746352805");
        System.out.println(res);
    }

    @Test
    public void test4() {
        HuobiProRest client = new HuobiProRest();
        List<Symbol> res = client.getSymbols();
        System.out.println(res);
    }

    @Test
    public void test5() {
        HuobiProRest client = new HuobiProRest();
        GetOrdersRequest req = new GetOrdersRequest();
        req.setSymbol("iotausdt");
        req.setStates("filled,canceled");
        ApiResponse<List<OrderDetail>> res = client.getRecentOrders(req);
        System.out.println(res);
    }

    @Test
    public void test6() throws OrderPlaceException, CancelOrderException {
        String accountId = "2672827";
        HuobiTriangleArbitrage triangleArbitrage = new HuobiTriangleArbitrage(accountId);
        triangleArbitrage.init();

        String symbol = "smteth";
        OrderPlaceRequest buy = new OrderPlaceRequest();
        buy.setAccountId(accountId);

        double base = triangleArbitrage.buyLimit(buy, 380, 0.00005557, symbol);
        System.out.println(base);
    }

    @Test
    public void test7() throws OrderPlaceException, InterruptedException, CancelOrderException {
        String accountId = "2672827";
        HuobiTriangleArbitrage triangleArbitrage = new HuobiTriangleArbitrage(accountId);
        triangleArbitrage.init();

        Thread.sleep(2000);
        String symbol = "smteth";
        OrderPlaceRequest sell = new OrderPlaceRequest();
        sell.setAccountId(accountId);

        double base = triangleArbitrage.sellLimit(sell, 373, 0.00005616, symbol);
        System.out.println(base);
    }

    @Test
    public void test8() {
        HuobiTriangleArbitrage strategy = new HuobiTriangleArbitrage("2672827");
        double amount = strategy.getCapitalFromBalance("eth");
        System.out.println(amount);
    }

}
