package io.magicalne.smym.strategy.integration;

import io.magicalne.smym.dto.Depth;
import io.magicalne.smym.dto.Triangular;
import io.magicalne.smym.exchanges.HuobiExchange;
import io.magicalne.smym.strategy.HuobiTriangleArbitrage;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TriangleArbitrageTest {

    public static final String ACCOUNT_ID = "2672827";
    @Before
    public void setup() {
    }

    @Test
    public void test1() throws InterruptedException {
        String accessKeyId = System.getenv("HUOBI_ACCESS_KEY");
        String accessKeySecret = System.getenv("HUOBI_ACCESS_KEY_SECRET");
        HuobiTriangleArbitrage strategy = new HuobiTriangleArbitrage("2672827", accessKeyId, accessKeySecret);
        strategy.init();
        //ethusdt: 289.29 -> veneth: 0.00289985 -> venusdt: 0.79, profit: 1.05595937430020
        Triangular triangular = new Triangular("ethusdt", "veneth", "venusdt");
        strategy.takeIt(triangular, 289.29, 0.79, 1.05595937, "3", true);
    }

    @Test
    public void test9() throws InterruptedException {
        String accessKey = System.getenv("BINANCE_ACCESS_KEY");
        String secretKey = System.getenv("BINANCE_ACCESS_SECRET_KEY");
        HuobiExchange huobiExchange = new HuobiExchange(ACCOUNT_ID, accessKey, secretKey);
        Set<String> symbols = new HashSet<>();
        symbols.add("leteth");
        huobiExchange.createOrderBook(symbols, 5);

        for (;;) {
            Depth depth = huobiExchange.getOrderBook("leteth");
            if (depth == null) {
                continue;
            }
            List<List<Double>> asks = depth.getAsks();
            System.out.println("ask################");
            asks.forEach(e -> System.out.println("price:" + e.get(0) + ", qty:" + e.get(1)));
            System.out.println("bid################");
            List<List<Double>> bids = depth.getBids();
            bids.forEach(e -> System.out.println("price:" + e.get(0) + ", qty:" + e.get(1)));
            Thread.sleep(5000);

        }
    }
}
