package io.magicalne.smym.strategy.integration;

import com.binance.api.client.domain.market.OrderBook;
import com.binance.api.client.domain.market.OrderBookEntry;
import io.magicalne.smym.exchanges.BinanceExchange;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BinanceTriangleArbitrageTest {

    private BinanceExchange exchange;

    @Before
    public void setup() {
        String accessKey = System.getenv("BINANCE_ACCESS_KEY");
        String secretKey = System.getenv("BINANCE_ACCESS_SECRET_KEY");
        this.exchange = new BinanceExchange(accessKey, secretKey);
    }

    @Test
    public void test1() throws InterruptedException {
        Set<String> symbols = new HashSet<>();
        String btcusdt = "ETHUSDT";
        symbols.add(btcusdt);
        this.exchange.createLocalOrderBook(symbols, 5);
        for (;;) {
            OrderBook orderBook = this.exchange.getOrderBook(btcusdt);
            List<OrderBookEntry> asks = orderBook.getAsks();
            System.out.println("ASK:");
            asks.forEach(System.out::println);

            List<OrderBookEntry> bids = orderBook.getBids();
            System.out.println("BID:");
            bids.forEach(System.out::println);
            System.out.println("#################");
            Thread.sleep(5000);
        }
    }

    @Test
    public void test2() {
        OrderBook orderbook = this.exchange.getOrderBookSnapshot("ETHUSDT", 10);
        List<OrderBookEntry> bids = orderbook.getBids();
        System.out.println("BID:");
        bids.forEach(System.out::println);

        List<OrderBookEntry> asks = orderbook.getAsks();
        System.out.println("ASK:");
        asks.forEach(System.out::println);
    }

}
