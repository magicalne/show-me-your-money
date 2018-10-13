package io.magicalne.smym.exchanges;

import com.binance.api.client.*;
import com.binance.api.client.domain.TimeInForce;
import com.binance.api.client.domain.account.Account;
import com.binance.api.client.domain.account.NewOrder;
import com.binance.api.client.domain.account.NewOrderResponse;
import com.binance.api.client.domain.account.Order;
import com.binance.api.client.domain.account.request.AllOrdersRequest;
import com.binance.api.client.domain.account.request.CancelOrderRequest;
import com.binance.api.client.domain.account.request.OrderStatusRequest;
import com.binance.api.client.domain.event.CandlestickEvent;
import com.binance.api.client.domain.event.DepthEvent;
import com.binance.api.client.domain.general.ExchangeInfo;
import com.binance.api.client.domain.general.FilterType;
import com.binance.api.client.domain.general.SymbolFilter;
import com.binance.api.client.domain.general.SymbolInfo;
import com.binance.api.client.domain.market.CandlestickInterval;
import com.binance.api.client.domain.market.OrderBook;
import com.binance.api.client.domain.market.OrderBookEntry;
import com.binance.api.client.exception.BinanceApiException;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class BinanceExchange {

  private final BinanceApiWebSocketClient wsClient;
  private final BinanceApiRestClient restClient;
  private final BinanceApiAsyncRestClient asyncRestClient;
  private ConcurrentMap<String, OrderBook> orderBookMap;
  private BinanceEventHandler<CandlestickEvent> candlestickHandler;
  private int orderBookSize;
  private ExchangeInfo exchangeInfo;

  public BinanceExchange(String accessKey, String secretKey) {
    BinanceApiClientFactory factory = BinanceApiClientFactory.newInstance(accessKey, secretKey);
    this.wsClient = factory.newWebSocketClient();
    this.restClient = factory.newRestClient();
    this.asyncRestClient = factory.newAsyncRestClient();
  }

  public void subscribeCandlestickEvent(Set<String> symbols, BinanceEventHandler<CandlestickEvent> handler) {
    this.candlestickHandler = handler;
    BinanceApiCallback<CandlestickEvent> callback = new UniverseApiCallback<CandlestickEvent>() {
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

  public List<Order> orderHistory(String symbol) {
    AllOrdersRequest req = new AllOrdersRequest(symbol);
    return this.restClient.getAllOrders(req);
  }

  public Order queryOrder(String symbol, long orderId) {
    return this.restClient.getOrderStatus(new OrderStatusRequest(symbol, orderId));
  }

  public NewOrderResponse marketSell(String symbol, String qty) {
    NewOrder newOrder = NewOrder.marketSell(symbol, qty);
    return this.restClient.newOrder(newOrder);
  }

  public NewOrderResponse marketBuy(String symbol, String qty) {
    NewOrder newOrder = NewOrder.marketBuy(symbol, qty);
    return this.restClient.newOrder(newOrder);
  }

  public NewOrderResponse limitBuy(String symbol, TimeInForce timeInForce, String quantity, String price) {
    NewOrder newOrder = NewOrder.limitBuy(symbol, timeInForce, quantity, price);
    return this.restClient.newOrder(newOrder);
  }

  public NewOrderResponse limitBuy(String symbol, TimeInForce timeInForce, String quantity, String price,
                                   long recv) {
    NewOrder newOrder = NewOrder.limitBuy(symbol, timeInForce, quantity, price).recvWindow(recv);
    return this.restClient.newOrder(newOrder);
  }

  public NewOrderResponse limitSell(String symbol, TimeInForce timeInForce, String quantity, String price) {
    NewOrder newOrder = NewOrder.limitSell(symbol, timeInForce, quantity, price);
    return this.restClient.newOrder(newOrder);
  }

  public NewOrderResponse limitSell(String symbol, TimeInForce timeInForce, String quantity, String price,
                                    long recvWindow) {
    NewOrder newOrder = NewOrder.limitSell(symbol, timeInForce, quantity, price).recvWindow(recvWindow);
    return this.restClient.newOrder(newOrder);
  }

  public void cancelOrder(String symbol, long orderId) {
    CancelOrderRequest request = new CancelOrderRequest(symbol, orderId);
    this.restClient.cancelOrder(request);
  }

  public boolean tryCancelOrder(String symbol, long orderId) {
    CancelOrderRequest request = new CancelOrderRequest(symbol, orderId);
    try {
      this.restClient.cancelOrder(request);
      return true;
    } catch(BinanceApiException e) {
      Order order = queryOrder(symbol, orderId);
      log.warn("Cannot cancel order: {}, {} due to {}", orderId, symbol, e);
      log.warn("Query order status: {}", order.getStatus());
      return false;
    }
  }

  public void createLocalOrderBook(Set<String> symbols, int size) {
    this.orderBookMap = new ConcurrentHashMap<>(symbols.size() / 3 * 4);
    this.orderBookSize = size;
    BinanceApiCallback<DepthEvent> callback = new UniverseApiCallback<DepthEvent>() {
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
    OrderBook orderBook = this.orderBookMap.get(symbol);
    List<OrderBookEntry> asks = event.getAsks();
    for (OrderBookEntry ask : asks) {
      if (new BigDecimal(ask.getQty()).stripTrailingZeros().equals(BigDecimal.ZERO)) {
        removePriceLevel(ask.getPrice(), orderBook.getAsks());
      } else {
        upsertPriceLevel(ask, orderBook.getAsks(), true);
      }
    }
    List<OrderBookEntry> bids = event.getBids();
    for (OrderBookEntry bid : bids) {
      if (new BigDecimal(bid.getQty()).stripTrailingZeros().equals(BigDecimal.ZERO)) {
        removePriceLevel(bid.getPrice(), orderBook.getBids());
      } else {
        upsertPriceLevel(bid, orderBook.getBids(), false);
      }
    }
  }

  private void upsertPriceLevel(OrderBookEntry entry, List<OrderBookEntry> orderBookEntries, boolean ascending) {
    for (OrderBookEntry e : orderBookEntries) {
      if (entry.getPrice().equals(e.getPrice())) {
        e.setQty(entry.getQty());
        return;
      }
    }
    orderBookEntries.add(entry);
    Comparator<OrderBookEntry> sortAsc = (e1, e2) -> {
      double p1 = Double.parseDouble(e1.getPrice());
      double p2 = Double.parseDouble(e2.getPrice());
      if (p1 - p2 > 0) {
        return 1;
      }
      if (p1 - p2 < 0) {
        return -1;
      }
      return 0;
    };

    Comparator<OrderBookEntry> sortDesc = (e1, e2) -> {
      double p1 = Double.parseDouble(e1.getPrice());
      double p2 = Double.parseDouble(e2.getPrice());
      if (p1 - p2 > 0) {
        return -1;
      }
      if (p1 - p2 < 0) {
        return 1;
      }
      return 0;
    };
    orderBookEntries.sort(ascending ? sortAsc : sortDesc);
    int size = orderBookEntries.size();
    if (size > orderBookSize) {
      orderBookEntries.remove(size - 1);
    }
  }

  private void removePriceLevel(String price, List<OrderBookEntry> orderBookEntries) {
    int index = 0;
    for (OrderBookEntry e : orderBookEntries) {
      if (price.equals(e.getPrice())) {
        orderBookEntries.remove(index);
        return;
      }
      index++;
    }
  }

  public ExchangeInfo getExchangeInfo() {
    if (exchangeInfo == null) {
      this.exchangeInfo = this.restClient.getExchangeInfo();
    }
    return this.exchangeInfo;
  }

  public OrderBookEntry getBestAsk(String symbol) {
    OrderBook orderBook = this.orderBookMap.get(symbol);
    if (orderBook != null) {
      List<OrderBookEntry> asks = orderBook.getAsks();
      if (asks != null && !asks.isEmpty()) {
        return asks.get(0);
      }
    }
    return null;
  }

  public OrderBookEntry getBestBid(String symbol) {
    OrderBook orderBook = this.orderBookMap.get(symbol);
    if (orderBook != null) {
      List<OrderBookEntry> bids = orderBook.getBids();
      if (bids != null && !bids.isEmpty()) {
        return bids.get(0);
      }
    }
    return null;
  }

  public int getQtyPrecision(String symbol) {
    SymbolInfo symbolInfo = getExchangeInfo().getSymbolInfo(symbol);
    SymbolFilter lotSize = symbolInfo.getSymbolFilter(FilterType.LOT_SIZE);
    String minQty = lotSize.getMinQty();
    int index = minQty.indexOf('1');
    return index == 0 ? index : index - 1;
  }

  public int getPricePrecision(String symbol) {
    SymbolInfo symbolInfo = getExchangeInfo().getSymbolInfo(symbol);
    SymbolFilter priceFilter = symbolInfo.getSymbolFilter(FilterType.PRICE_FILTER);
    String tickSize = priceFilter.getTickSize();
    int index = tickSize.indexOf("1");
    return index == 0 ? index : index - 1;
  }
}
