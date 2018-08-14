package io.magicalne.smym.exchanges;

import com.binance.api.client.BinanceApiCallback;
import com.binance.api.client.BinanceApiClientFactory;
import com.binance.api.client.BinanceApiRestClient;
import com.binance.api.client.BinanceApiWebSocketClient;
import com.binance.api.client.domain.account.Account;
import com.binance.api.client.domain.event.CandlestickEvent;
import com.binance.api.client.domain.event.DepthEvent;
import com.binance.api.client.domain.general.ExchangeInfo;
import com.binance.api.client.domain.market.CandlestickInterval;
import com.binance.api.client.domain.market.OrderBook;
import com.binance.api.client.domain.market.OrderBookEntry;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class BinanceExchange {

    private final BinanceApiWebSocketClient wsClient;
    private final BinanceApiRestClient restClient;
    private final ConcurrentMap<String, OrderBook> orderBookMap;
    private BinanceEventHandler<CandlestickEvent> candlestickHandler;

    public BinanceExchange(String accessKey, String secretKey) {
        BinanceApiClientFactory factory = BinanceApiClientFactory.newInstance(accessKey, secretKey);
        this.wsClient = factory.newWebSocketClient();
        this.restClient = factory.newRestClient();
        this.orderBookMap = new ConcurrentHashMap<>();
    }

    public void subscribeCandlestickEvent(Set<String> symbols, BinanceEventHandler<CandlestickEvent> handler) {
        this.candlestickHandler = handler;
        BinanceApiCallback<CandlestickEvent> callback = new BinanceApiCallbackWrapper<CandlestickEvent>() {
            @Override
            public void onResponse(CandlestickEvent candlestickEvent) {
                candlestickHandler.update(candlestickEvent.getSymbol(), candlestickEvent);
            }
        };
        for (String s : symbols) {
            this.wsClient.onCandlestickEvent(s.toLowerCase(), CandlestickInterval.ONE_MINUTE, callback);
        }
        log.info("Subscribe {} candle stick event.", symbols.size());
    }

    public Account getAccount() {
        return this.restClient.getAccount();
    }

    public OrderBook getOrderBookSnapshot(String symbol, int size) {
        return this.restClient.getOrderBook(symbol, size);
    }

    public OrderBook getOrderBook(String symbol) {
        return this.orderBookMap.get(symbol);
    }

    public void createLocalOrderBook(Set<String> symbols, int size) {
        BinanceApiCallback<DepthEvent> callback = new BinanceApiCallbackWrapper<DepthEvent>() {
            @Override
            public void onResponse(DepthEvent event) {
                updateOrderBook(event);
            }
        };
        for (String symbol : symbols) {
            OrderBook orderBook = this.restClient.getOrderBook(symbol, size);
            this.orderBookMap.put(symbol, orderBook);
            this.wsClient.onDepthEvent(symbol.toLowerCase(), callback);
        }
        log.info("Create {} market order books.", symbols.size());
    }

    private void updateOrderBook(DepthEvent event) {
        String symbol = event.getSymbol();
        long firstUpdateId = event.getFirstUpdateId();
        long finalUpdateId = event.getFinalUpdateId();
        OrderBook orderBook = this.orderBookMap.get(symbol);
        long lastUpdateId = orderBook.getLastUpdateId();
        if (finalUpdateId <= lastUpdateId) {
            return;
        }
        if (firstUpdateId <= lastUpdateId + 1 && finalUpdateId >= lastUpdateId + 1) {
            List<OrderBookEntry> asks = event.getAsks();
            for (OrderBookEntry ask : asks) {
                if ("0".equals(ask.getQty())) {
                    removePriceLevel(ask.getPrice(), orderBook.getAsks());
                } else {
                    upsertPriceLevel(ask, orderBook.getAsks());
                }
            }
            List<OrderBookEntry> bids = event.getBids();
            for (OrderBookEntry bid : bids) {
                if ("0".equals(bid.getQty())) {
                    removePriceLevel(bid.getPrice(), orderBook.getBids());
                } else {
                    upsertPriceLevel(bid, orderBook.getBids());
                }
            }
        }
    }

    private void upsertPriceLevel(OrderBookEntry entry, List<OrderBookEntry> orderBookEntries) {
        for (OrderBookEntry e : orderBookEntries) {
            if (entry.getPrice().equals(e.getPrice())) {
                e.setQty(entry.getQty());
                return;
            }
        }
        orderBookEntries.add(entry);
        orderBookEntries.sort((e1, e2) -> {
            double p1 = Double.parseDouble(e1.getPrice());
            double p2 = Double.parseDouble(e2.getPrice());
            if (p1 - p2 > 0) {
                return 1;
            }
            if (p1 - p2 < 0) {
                return -1;
            }
            return 0;
        });
    }

    private void removePriceLevel(String price, List<OrderBookEntry> orderBookEntries) {
        int index = 0;
        for (OrderBookEntry e : orderBookEntries) {
            if (price.equals(e.getPrice())) {
                orderBookEntries.remove(index);
                return;
            }
            index ++;
        }
    }

    public ExchangeInfo getExchangeInfo() {
        return this.restClient.getExchangeInfo();
    }
}
