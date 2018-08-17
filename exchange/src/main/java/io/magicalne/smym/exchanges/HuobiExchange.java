package io.magicalne.smym.exchanges;

import io.magicalne.smym.dto.*;
import io.magicalne.smym.exception.ApiException;
import io.magicalne.smym.exchanges.huobi.HuobiApiClientFactory;
import io.magicalne.smym.exchanges.huobi.HuobiProRest;
import io.magicalne.smym.exchanges.huobi.HuobiProWebSocketClient;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class HuobiExchange {

    public static final String CANCELED = "canceled";
    private static final String FILLED = "filled";
    public static final String PARTIAL_CANCELED = "partial-canceled";
    private final HuobiProRest restClient;
    private final HuobiProWebSocketClient webSocketClient;
    private final String accountId;

    private ConcurrentMap<String, Depth> orderBookMap;
    private int orderBookSize;

    public HuobiExchange(String accountId, String accessKey, String secretKey) {
        this.accountId = accountId;
        HuobiApiClientFactory instance = HuobiApiClientFactory.newInstance(accessKey, secretKey);
        this.restClient = instance.createRestClient();
        this.webSocketClient = instance.createWebSocketClient();
    }

    public List<Symbol> getSymbolInfo() {
        return this.restClient.getSymbols();
    }

    public BalanceResponse getAccount(String accountId) {
        return this.restClient.balance(accountId);
    }

    public List<Double> getBestAsk(String symbol) {
        Depth depth = this.orderBookMap.get(symbol);
        List<List<Double>> asks = depth.getAsks();
        if (asks != null && !asks.isEmpty()) {
            return asks.get(0);
        } else {
            return null;
        }
    }

    public List<Double> getBestBid(String symbol) {
        Depth depth = this.orderBookMap.get(symbol);
        List<List<Double>> bids = depth.getBids();
        if (bids != null && !bids.isEmpty()) {
            return bids.get(0);
        } else {
            return null;
        }
    }

    public Depth getOrderBook(String symbol) {
        return this.orderBookMap.get(symbol);
    }

    public void createOrderBook(Set<String> symbols, int size) {
        this.orderBookMap = new ConcurrentHashMap<>(symbols.size() / 3 * 4);
        this.orderBookSize = size;

        UniverseApiCallback<DepthResponse> callback = new UniverseApiCallback<DepthResponse>() {
            @Override
            public void onResponse(DepthResponse depth) {
                String ch = depth.getCh();
                String symbolFromTopic = getSymbolFromTopic(ch);
                updateOrderBook(symbolFromTopic, depth.getTick());
            }
        };
        this.webSocketClient.onDepthEvent(symbols, callback);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            log.error("Thread sleep with interrupted exception.", e);
        }
        log.info("Create {} market order books.", symbols.size());
    }

    private String getSymbolFromTopic(String topic) {
        //market.btcusdt.depth.step5
        int start = topic.indexOf('.');
        int end = topic.indexOf('.', start + 1);
        return topic.substring(start + 1, end);
    }

    private void updateOrderBook(String symbol, Depth depth) {
        this.orderBookMap.put(symbol, depth);
    }

    private OrderPlaceResponse order(String symbol, String quantity, String price, OrderType orderType) {
        OrderPlaceRequest req = new OrderPlaceRequest();
        req.setAccountId(accountId);
        req.setSymbol(symbol);
        req.setType(orderType.getType());
        req.setPrice(price);
        req.setAmount(quantity);
        return this.restClient.orderPlace(req);
    }

    public OrderDetail marketBuy(String symbol, String quoteQuantity) {
        OrderPlaceResponse res = order(symbol, quoteQuantity, null, OrderType.BUY_MARKET);
        if (res.checkStatusOK()) {
            String orderId = res.getData();
            for (;;) {
                OrderDetail detail = queryOrder(orderId);
                if (FILLED.equals(detail.getState())) {
                    return detail;
                }
            }
        } else {
            return marketBuy(symbol, quoteQuantity);
        }
    }

    public OrderDetail marketSell(String symbol, String baseQuantity) {
        OrderPlaceResponse res = order(symbol, baseQuantity, null, OrderType.SELL_MARKET);
        if (res.checkStatusOK()) {
            String orderId = res.getData();
            for (;;) {
                OrderDetail detail = queryOrder(orderId);
                if (FILLED.equals(detail.getState())) {
                    return detail;
                }
            }
        } else {
            return marketBuy(symbol, baseQuantity);
        }
    }

    public OrderPlaceResponse limitBuy(String symbol, String quantity, String price) {
        return order(symbol, quantity, price, OrderType.BUY_LIMIT);
    }

    public OrderPlaceResponse limitSell(String symbol, String quantity, String price) {
        return order(symbol, quantity, price, OrderType.SELL_LIMIT);
    }

    public OrderDetail cancel(String orderId) {
        boolean status = this.restClient.submitcancel(orderId).checkStatusOK();
        if (status) {
            for (;;) {
                OrderDetail detail = queryOrder(orderId);
                String state = detail.getState();
                if (CANCELED.equals(state) ||
                        PARTIAL_CANCELED.equals(state) ||
                        FILLED.equals(state)) {
                    return detail;
                }
            }
        } else {
            return cancel(orderId);
        }
    }

    public OrderDetail queryOrder(String orderId) {
        OrdersDetailResponse res = this.restClient.ordersDetail(orderId);
        if (res.checkStatusOK()) {
            return res.getData();
        } else {
            throw new ApiException(res.getErrCode(), res.getErrMsg());
        }
    }

}
