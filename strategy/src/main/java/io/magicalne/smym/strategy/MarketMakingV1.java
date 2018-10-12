package io.magicalne.smym.strategy;

import com.binance.api.client.domain.OrderStatus;
import com.binance.api.client.domain.TimeInForce;
import com.binance.api.client.domain.account.NewOrderResponse;
import com.binance.api.client.domain.account.Order;
import io.magicalne.smym.exchanges.BinanceExchange;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
public class MarketMakingV1 {

  private final AtomicInteger buys = new AtomicInteger(0);
  private final BinanceExchange exchange;
  private final String symbol;
  private final String qtyUnit;
  private final BigDecimal gridRate;
  private final int gridSize;
  private final String initPrice;

  public MarketMakingV1(String accessId, String secretKey,
                        String symbol, String qtyUnit, String gridRate, int gridSize, String initPrice) {
    this.symbol = symbol;
    this.qtyUnit = qtyUnit;
    this.gridRate = new BigDecimal(gridRate);
    this.gridSize = gridSize;
    this.initPrice = initPrice;
    this.exchange = new BinanceExchange(accessId, secretKey);
  }

  public void execute()
    throws InterruptedException {
    OrderInfo orderInfo = placeOrdersInGrid();
    log.info("Init order: {}", orderInfo);

    for (;;) {
      checkFilledOrder(orderInfo);
      Thread.sleep(1000);
    }
  }

  private void checkFilledOrder(OrderInfo orderInfo) {
    int pricePrecision = this.exchange.getPricePrecision(symbol);

    //check buy orders
    NewOrderResponse firstBid = orderInfo.bidOrders.getFirst();
    Order order = this.exchange.queryOrder(symbol, firstBid.getOrderId());
    if (order.getStatus() == OrderStatus.FILLED && this.buys.get() < gridSize) {
      log.info("Buy {} - {} - {}", symbol, order.getPrice(), order.getExecutedQty());
      orderInfo.removeBidOrdersHead();
      buys.incrementAndGet();

      //place new bid order to tail
      NewOrderResponse bidOrdersTail = orderInfo.getBidOrdersTail();
      BigDecimal bot = new BigDecimal(bidOrdersTail.getPrice());
      BigDecimal newBid = bot.multiply(gridRate).setScale(pricePrecision, RoundingMode.HALF_EVEN);
      NewOrderResponse newBidOrder = this.exchange.limitBuy(symbol, TimeInForce.GTC, qtyUnit, newBid.toPlainString());
      orderInfo.addBidOrder(newBidOrder);

      //cancel tail ask order and place new ask order to head
      NewOrderResponse lastAsk = orderInfo.askOrders.getLast();
      this.exchange.cancelOrder(symbol, lastAsk.getOrderId());
      //add new ask order based on existed lowest ask price(header) to the head
      NewOrderResponse firstAsk = orderInfo.askOrders.getFirst();
      BigDecimal newPrice = new BigDecimal(firstAsk.getPrice()).divide(gridRate, RoundingMode.HALF_EVEN)
        .setScale(pricePrecision, RoundingMode.HALF_EVEN);
      NewOrderResponse newOrder = this.exchange.limitSell(symbol, TimeInForce.GTC, qtyUnit, newPrice.toPlainString());
      orderInfo.addAskOrderToHead(newOrder);
      log.info("Grid: {}", orderInfo);
    }

    //check sell orders
    NewOrderResponse firstAsk = orderInfo.askOrders.getFirst();
    order = this.exchange.queryOrder(symbol, firstAsk.getOrderId());
    if (order.getStatus() == OrderStatus.FILLED && this.buys.get() > -gridSize) {
      log.info("Sell {} - {} - {}", symbol, order.getPrice(), order.getExecutedQty());
      orderInfo.removeAskOrdersHead();
      buys.decrementAndGet();

      //place new ask order to tail
      NewOrderResponse askOrdersTail = orderInfo.getAskOrdersTail();
      BigDecimal aot = new BigDecimal(askOrdersTail.getPrice());
      BigDecimal newAsk = aot.divide(gridRate, RoundingMode.HALF_EVEN).setScale(pricePrecision, RoundingMode.HALF_EVEN);
      NewOrderResponse newAskOrder = this.exchange.limitSell(symbol, TimeInForce.GTC, qtyUnit, newAsk.toPlainString());
      orderInfo.addAskOrder(newAskOrder);

      //cancel tail bid order and place new bid order to head
      NewOrderResponse lastBid = orderInfo.bidOrders.getLast();
      this.exchange.cancelOrder(symbol, lastBid.getOrderId());
      //add new bid order based on existed highest bid price(header) to the head
      firstBid = orderInfo.getBidOrders().getFirst();
      BigDecimal newPrice = new BigDecimal(firstBid.getPrice()).divide(gridRate, RoundingMode.HALF_EVEN)
        .setScale(pricePrecision, RoundingMode.HALF_EVEN);
      NewOrderResponse newOrder = this.exchange.limitBuy(symbol, TimeInForce.GTC, qtyUnit, newPrice.toPlainString());
      orderInfo.addBidOrderToHead(newOrder);
      log.info("Grid: {}", orderInfo);
    }
  }

  private OrderInfo placeOrdersInGrid() {
    int pricePrecision = this.exchange.getPricePrecision(symbol);
    BigDecimal iPrice = new BigDecimal(initPrice).setScale(pricePrecision, RoundingMode.HALF_EVEN);

    OrderInfo orderInfo = new OrderInfo();
    //place buy orders
    BigDecimal p = new BigDecimal(iPrice.toPlainString());
    for (int i = 0; i < gridSize; i ++) {
      p = p.divide(gridRate, RoundingMode.HALF_EVEN).setScale(pricePrecision, RoundingMode.HALF_EVEN);
      NewOrderResponse order = this.exchange.limitBuy(symbol, TimeInForce.GTC, qtyUnit, p.toPlainString());
      orderInfo.addBidOrder(order);
    }
    //place sell orders
    p = new BigDecimal(iPrice.toPlainString());
    for (int i = 0; i < gridSize; i ++) {
      p = p.multiply(gridRate).setScale(pricePrecision, RoundingMode.HALF_EVEN);
      NewOrderResponse order = this.exchange.limitSell(symbol, TimeInForce.GTC, qtyUnit, p.toPlainString());
      orderInfo.addAskOrder(order);
    }

    return orderInfo;
  }

  @Data
  private static class OrderInfo {
    private final LinkedList<NewOrderResponse> bidOrders;
    private final LinkedList<NewOrderResponse> askOrders;

    OrderInfo() {
      this.bidOrders = new LinkedList<>();
      this.askOrders = new LinkedList<>();
    }

    void addBidOrder(NewOrderResponse order) {
      this.bidOrders.add(order);
    }

    void addBidOrderToHead(NewOrderResponse order) {
      this.bidOrders.removeLast();
      this.bidOrders.addFirst(order);
    }

    void addAskOrder(NewOrderResponse order) {
      this.askOrders.add(order);
    }

    void addAskOrderToHead(NewOrderResponse order) {
      this.askOrders.removeLast();
      this.askOrders.addFirst(order);
    }

    void removeBidOrdersHead() {
      this.bidOrders.removeFirst();
    }

    void removeAskOrdersHead() {
      this.askOrders.removeFirst();
    }

    NewOrderResponse getBidOrdersTail() {
      return bidOrders.getLast();
    }

    NewOrderResponse getAskOrdersTail() {
      return askOrders.getLast();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("OrderInfo: \n")
        .append("bid: ");
      bidOrders.forEach(r -> sb.append(r.getOrderId()).append(": ").append(r.getPrice()).append(" "));
      sb.append("\n")
        .append("ask: ");
      askOrders.forEach(r -> sb.append(r.getOrderId()).append(": ").append(r.getPrice()).append(" "));

      return sb.toString();
    }
  }
}
