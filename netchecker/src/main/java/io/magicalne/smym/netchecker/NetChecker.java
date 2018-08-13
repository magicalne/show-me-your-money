package io.magicalne.smym.netchecker;

import io.magicalne.smym.dto.OrderPlaceRequest;
import io.magicalne.smym.dto.OrderType;
import io.magicalne.smym.exchanges.HuobiProRest;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NetChecker {

    private void latencyTest() {

        HuobiProRest client = new HuobiProRest();
        int round = 10;
        log.info("Starting test network latency. Total round: {}.", round+1);
        long totalStart = System.currentTimeMillis();
        for (int i = 0; i < round; i ++) {
            log.info("######Round {}######:", i);
            long roundStart = System.currentTimeMillis();

            long start = System.currentTimeMillis();
            client.getSymbols();
            long end = System.currentTimeMillis();
            log.info("Test getSymbol API: {}ms", end - start);

            start = System.currentTimeMillis();
            client.getAccounts();
            end = System.currentTimeMillis();
            log.info("Test getAccounts API: {}ms", end - start);

            start = System.currentTimeMillis();
            String symbol = "btcusdt";
            client.trade(symbol);
            end = System.currentTimeMillis();
            log.info("Test get trade({}) API: {}ms", symbol, end - start);

            start = System.currentTimeMillis();
            client.detail(symbol);
            end = System.currentTimeMillis();
            log.info("Test detail({}) API: {}ms", symbol, end - start);

            start = System.currentTimeMillis();
            client.getAccounts();
            end = System.currentTimeMillis();
            log.info("Test getAccounts API: {}ms", end - start);

            start = System.currentTimeMillis();
            client.timestamp();
            end = System.currentTimeMillis();
            log.info("Test timestamp API: {}ms", end - start);

            start = System.currentTimeMillis();
            client.historyTrade(symbol, "2000");
            end = System.currentTimeMillis();
            log.info("Test historyTrade({}, 2000) API: {}ms", symbol, end - start);

            log.info("#####Round {} ended. Cost {}ms#####", i, System.currentTimeMillis() - roundStart);
        }

        log.info("Test trade API...");
        for (int i = 0; i < 10; i ++) {
            long start = System.currentTimeMillis();
            OrderPlaceRequest req = new OrderPlaceRequest();
            req.setAccountId("test");
            req.setAmount("1");
            req.setSymbol("btcusdt");
            req.setType(OrderType.BUY_MARKET.getType());
            client.orderPlace(req);
            long end = System.currentTimeMillis();
            log.info("Round {}, cost {}ms", i+1, end - start);
        }
        long totalEnd = System.currentTimeMillis();
        long total = totalEnd - totalStart;
        log.info("Test ended. Total cost: {}ms, avg: {}ms", total, total / round);
    }

    public static void main(String[] args) {
        NetChecker netChecker = new NetChecker();
        netChecker.latencyTest();
    }
}
