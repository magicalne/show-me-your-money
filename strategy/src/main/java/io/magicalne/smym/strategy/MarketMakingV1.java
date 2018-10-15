package io.magicalne.smym.strategy;

import com.binance.api.client.domain.OrderStatus;
import com.binance.api.client.domain.TimeInForce;
import com.binance.api.client.domain.account.NewOrderResponse;
import com.binance.api.client.domain.account.Order;
import com.binance.api.client.exception.BinanceApiException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import io.magicalne.smym.dto.GridTradeConfig;
import io.magicalne.smym.dto.MarketMakingConfig;
import io.magicalne.smym.exchanges.BinanceExchange;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


@Slf4j
public class MarketMakingV1 {

  private final BinanceExchange exchange;
  private final List<GridTrading> gridTradings;

  public MarketMakingV1(String accessId, String secretKey, String path) throws IOException {
    this.exchange = new BinanceExchange(accessId, secretKey);
    MarketMakingConfig config = readYaml(path);
    gridTradings = init(config);
  }

  public void execute() throws InterruptedException {
    log.info("Grid trading config: {}", gridTradings);
    List<OrderInfo> orderInfos = new LinkedList<>();
    for (GridTrading gridTrading : gridTradings) {
      try {
        OrderInfo orderInfo = gridTrading.placeOrdersInGrid();
        orderInfos.add(orderInfo);
      } catch (BinanceApiException e) {
        log.error("Binance api exception: ", e);
      }
      Thread.sleep(1000); //In case of "Too many new orders; current limit is 10 orders per SECOND".
    }

    for (;;) {
      for (int i = 0; i < gridTradings.size(); i ++) {
        GridTrading gridTrading = gridTradings.get(i);
        OrderInfo orderInfo = orderInfos.get(i);
        try {
          gridTrading.checkFilledOrder(orderInfo);
        } catch (Exception e) {
          log.error("Some exception happened during trading.", e);
        }
      }
      Thread.sleep(1000);
    }
  }
  private List<GridTrading> init(MarketMakingConfig config) {
    List<GridTradeConfig> grids = config.getGrids();
    String errMsg = "There is no grid trading config!";
    Preconditions.checkArgument(grids != null && !grids.isEmpty(), errMsg);

    return grids.stream().map(g -> new GridTrading(exchange, g)).collect(Collectors.toList());
  }

  private MarketMakingConfig readYaml(String path) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    return mapper.readValue(new File(path), MarketMakingConfig.class);
  }

  private static class GridTrading {
    private final AtomicInteger buys = new AtomicInteger(0);
    private final BinanceExchange exchange;
    private final String symbol;
    private final String qtyUnit;
    private final BigDecimal gridRate;
    private final int gridSize;
    private final String initPrice;

    GridTrading(BinanceExchange exchange, GridTradeConfig config) {
      this.exchange = exchange;
      this.symbol = config.getSymbol();
      this.qtyUnit = config.getQtyUnit();
      this.gridRate = new BigDecimal(config.getGridRate());
      this.gridSize = config.getGridSize();
      this.initPrice = config.getInitPrice();
    }

    private void checkFilledBidOrder(OrderInfo orderInfo, int pricePrecision) {
      NewOrderResponse firstBid = orderInfo.bidOrders.getFirst();
      Order order = this.exchange.queryOrder(symbol, firstBid.getOrderId());
      if (order.getStatus() == OrderStatus.FILLED && this.buys.get() < gridSize) {
        log.info("Buy {} - {} - {}, order id: {}",
          symbol, order.getPrice(), order.getExecutedQty(), order.getOrderId());
        orderInfo.removeBidOrdersHead();
        buys.incrementAndGet();

        //place new bid order to tail
        NewOrderResponse bidOrdersTail = orderInfo.getBidOrdersTail();
        BigDecimal bot = new BigDecimal(bidOrdersTail.getPrice());
        BigDecimal newBid = bot.divide(gridRate, RoundingMode.HALF_EVEN)
          .setScale(pricePrecision, RoundingMode.HALF_EVEN);
        NewOrderResponse newBidOrder =
          this.exchange.limitBuy(symbol, TimeInForce.GTC, qtyUnit, newBid.toPlainString());
        orderInfo.addBidOrder(newBidOrder);

        //cancel tail ask order and place new ask order to head
        NewOrderResponse lastAsk = orderInfo.askOrders.getLast();
        boolean success = this.exchange.tryCancelOrder(symbol, lastAsk.getOrderId());
        if (success) {
          orderInfo.removeAskOrdersTail();
          //add new ask order based on existed lowest ask price(header) to the head
          NewOrderResponse firstAsk = orderInfo.askOrders.getFirst();
          BigDecimal newPrice = new BigDecimal(firstAsk.getPrice()).divide(gridRate, RoundingMode.HALF_EVEN)
            .setScale(pricePrecision, RoundingMode.HALF_EVEN);
          NewOrderResponse newOrder =
            this.exchange.limitSell(symbol, TimeInForce.GTC, qtyUnit, newPrice.toPlainString());
          orderInfo.addAskOrderToHead(newOrder);
        }
        log.info("Grid: {}", orderInfo);
      }
    }

    private void checkFilledAskOrder(OrderInfo orderInfo, int pricePrecision) {
      NewOrderResponse firstAsk = orderInfo.askOrders.getFirst();
      Order order = this.exchange.queryOrder(symbol, firstAsk.getOrderId());
      if (order.getStatus() == OrderStatus.FILLED && this.buys.get() > -gridSize) {
        log.info("Sell {} - {} - {}, order id: {}",
          symbol, order.getPrice(), order.getExecutedQty(), order.getOrderId());
        orderInfo.removeAskOrdersHead();
        buys.decrementAndGet();

        //place new ask order to tail
        NewOrderResponse askOrdersTail = orderInfo.getAskOrdersTail();
        BigDecimal aot = new BigDecimal(askOrdersTail.getPrice());
        BigDecimal newAsk = aot.multiply(gridRate).setScale(pricePrecision, RoundingMode.HALF_EVEN);
        NewOrderResponse newAskOrder =
          this.exchange.limitSell(symbol, TimeInForce.GTC, qtyUnit, newAsk.toPlainString());
        orderInfo.addAskOrder(newAskOrder);

        //cancel tail bid order and place new bid order to head
        NewOrderResponse lastBid = orderInfo.bidOrders.getLast();
        boolean success = this.exchange.tryCancelOrder(symbol, lastBid.getOrderId());
        if (success) {
          orderInfo.removeBidOrdersTail();
          //add new bid order based on existed highest bid price(header) to the head
          NewOrderResponse firstBid = orderInfo.getBidOrders().getFirst();
          BigDecimal newPrice = new BigDecimal(firstBid.getPrice()).multiply(gridRate)
            .setScale(pricePrecision, RoundingMode.HALF_EVEN);
          NewOrderResponse newOrder =
            this.exchange.limitBuy(symbol, TimeInForce.GTC, qtyUnit, newPrice.toPlainString());
          orderInfo.addBidOrderToHead(newOrder);
        } else {
          Order lastBidOrder = this.exchange.queryOrder(symbol, lastBid.getOrderId());
          if (lastBidOrder.getStatus() == OrderStatus.FILLED && this.buys.get() < gridSize) {

          }
        }
        log.info("Grid: {}", orderInfo);
      }
    }

    private void checkFilledOrder(OrderInfo orderInfo) {
      int pricePrecision = this.exchange.getPricePrecision(symbol);

      //check buy orders
      checkFilledBidOrder(orderInfo, pricePrecision);

      //check sell orders
      checkFilledAskOrder(orderInfo, pricePrecision);
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
      log.info("Init order: {}", orderInfo);
      return orderInfo;
    }

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
      this.bidOrders.addLast(order);
    }

    void addBidOrderToHead(NewOrderResponse order) {
      this.bidOrders.removeLast();
      this.bidOrders.addFirst(order);
    }

    void addAskOrder(NewOrderResponse order) {
      this.askOrders.addLast(order);
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

    void removeBidOrdersTail() {
      this.bidOrders.removeLast();
    }

    void removeAskOrdersTail() {
      this.askOrders.removeLast();
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
