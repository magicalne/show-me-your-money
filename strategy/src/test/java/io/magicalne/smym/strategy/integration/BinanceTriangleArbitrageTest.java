package io.magicalne.smym.strategy.integration;

import com.binance.api.client.domain.TimeInForce;
import com.binance.api.client.domain.account.Account;
import com.binance.api.client.domain.account.AssetBalance;
import com.binance.api.client.domain.account.NewOrderResponse;
import com.binance.api.client.domain.account.Order;
import com.binance.api.client.domain.general.*;
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
        OrderBook orderbook = this.exchange.getOrderBookSnapshot("ETHBTC", 5);
        List<OrderBookEntry> bids = orderbook.getBids();
        System.out.println("BID:");
        bids.forEach(System.out::println);

        List<OrderBookEntry> asks = orderbook.getAsks();
        System.out.println("ASK:");
        asks.forEach(System.out::println);
    }

    @Test
    public void test3() {
        NewOrderResponse response = this.exchange.limitBuy("ETHUSDT", TimeInForce.IOC, "0.05", "300.99", 50000);
        System.out.println(response.getOrderId());
        System.out.println(response.getStatus());
    }

    @Test
    public void test4() {
        List<Order> orders = this.exchange.orderHistory("ETHBTC");
        for (Order order : orders) {
            System.out.println(order);
        }
    }

    @Test
    public void test5() {
        Order order = this.exchange.queryOrder("BTCUSDT", 151429335);
        System.out.println(order);
    }

    @Test
    public void test6() {
        Account account = this.exchange.getAccount();
        AssetBalance assetBalance = account.getAssetBalance("BTC");
        System.out.println(assetBalance);
    }

    @Test
    public void test7() {
        ExchangeInfo exchangeInfo = this.exchange.getExchangeInfo();
        SymbolInfo si = exchangeInfo.getSymbolInfo("BTCUSDT");
        SymbolFilter symbolFilter = si.getSymbolFilter(FilterType.LOT_SIZE);
        System.out.println(symbolFilter.getMinQty());
        SymbolFilter symbolFilter1 = si.getSymbolFilter(FilterType.MIN_NOTIONAL);
        System.out.println(symbolFilter1.getMinNotional());
        System.out.println(si.getBaseAssetPrecision());
        System.out.println(si.getQuotePrecision());
        SymbolFilter symbolFilter2 = si.getSymbolFilter(FilterType.PRICE_FILTER);
        System.out.println(symbolFilter2.getTickSize());

    }

    @Test
    public void test8() {
        ExchangeInfo exchangeInfo = this.exchange.getExchangeInfo();
        SymbolInfo btcusdt = exchangeInfo.getSymbolInfo("BTCUSDT");
        System.out.println(btcusdt.getBaseAsset());
    }

}
