package io.magicalne.smym.strategy.integration;

import io.magicalne.smym.exchanges.BinanceExchange;
import org.junit.Before;

public class BinanceTriangleArbitrageTest {

    private BinanceExchange exchange;
    private String accessKey;
    private String secretKey;
    @Before
    public void setup() {
        accessKey = System.getenv("BINANCE_ACCESS_KEY");
        secretKey = System.getenv("BINANCE_ACCESS_SECRET_KEY");
        this.exchange = new BinanceExchange(accessKey, secretKey);
    }

}
