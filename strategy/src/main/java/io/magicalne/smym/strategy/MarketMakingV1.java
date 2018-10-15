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
    for (GridTrading gridTrading : gridTradings) {
      try {
        gridTrading.placeOrdersInGrid();
      } catch (BinanceApiException e) {
        log.error("Binance api exception: ", e);
      }
      Thread.sleep(1000); //In case of "Too many new orders; current limit is 10 orders per SECOND".
    }

    for (;;) {
      for (GridTrading gridTrading : gridTradings) {
        try {
          gridTrading.checkFilledOrder();
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
    private static final double COMMISSION = 0.999;

    private final AtomicInteger buys = new AtomicInteger(0);
    private final BinanceExchange exchange;
    private final String symbol;
    private final String qtyUnit;
    private final BigDecimal gridRate;
    private final int gridSize;
    private final String initPrice;

    private Node bids;
    private Node asks;
    private int pricePrecision;
    private int profit = 0;

    GridTrading(BinanceExchange exchange, GridTradeConfig config) {
      this.exchange = exchange;
      this.symbol = config.getSymbol();
      this.qtyUnit = config.getQtyUnit();
      this.gridRate = new BigDecimal(config.getGridRate());
      this.gridSize = config.getGridSize();
      this.initPrice = config.getInitPrice();
    }

    private void checkBidOrderFilled(Node node) {
      NewOrderResponse bid = node.getValue();
      Order order = this.exchange.queryOrder(symbol, bid.getOrderId());
      if (order.getStatus() == OrderStatus.FILLED && this.buys.get() < gridSize) {
        buys.incrementAndGet();
        double price = Double.parseDouble(order.getPrice());
        double executedQty = Double.parseDouble(order.getExecutedQty());
        profit -= price * executedQty * COMMISSION;
        log.info("Buy {} - {} - {}, order id: {}, profit: {}", symbol, price, executedQty, order.getOrderId(), profit);
        //place new bid order to tail
        Node tail = getTail(bids);
        NewOrderResponse bidTail = tail.getValue();
        BigDecimal bot = new BigDecimal(bidTail.getPrice());
        BigDecimal newBid = bot.divide(gridRate, RoundingMode.HALF_EVEN)
          .setScale(pricePrecision, RoundingMode.HALF_EVEN);
        NewOrderResponse newBidOrder =
          this.exchange.limitBuy(symbol, TimeInForce.GTC, qtyUnit, newBid.toPlainString());
        tail.next = new Node(newBidOrder);
        //set bid head to next
        this.bids = bids.next;

        //cancel tail ask order and place new ask order to head
        Node askTail = getTail(asks);
        boolean success = this.exchange.tryCancelOrder(symbol, askTail.getValue().getOrderId());
        if (success) {
          removeTail(asks);
          //add new ask order based on existed lowest ask price(header) to the head
          NewOrderResponse firstAsk = asks.getValue();
          BigDecimal newPrice = new BigDecimal(firstAsk.getPrice()).divide(gridRate, RoundingMode.HALF_EVEN)
            .setScale(pricePrecision, RoundingMode.HALF_EVEN);
          NewOrderResponse newOrder =
            this.exchange.limitSell(symbol, TimeInForce.GTC, qtyUnit, newPrice.toPlainString());
          Node newAsk = new Node(newOrder);
          newAsk.next = asks;
          asks = newAsk;
        }
        log.info("{}, bids: {}", symbol, bids);
      }
      if (node.getNext() != null) {
        checkBidOrderFilled(node.getNext());
      }
    }

    private void checkAskOrderFilled(Node node) {
      NewOrderResponse ask = node.getValue();
      Order order = this.exchange.queryOrder(symbol, ask.getOrderId());
      if (order.getStatus() == OrderStatus.FILLED && this.buys.get() > -gridSize) {
        buys.decrementAndGet();
        double price = Double.parseDouble(order.getPrice());
        double executedQty = Double.parseDouble(order.getExecutedQty());
        profit += price * executedQty * COMMISSION;
        log.info("Sell {} - {} - {}, order id: {}, profit: {}", symbol, price, executedQty, order.getOrderId(), profit);
        //place new ask order to tail
        Node tail = getTail(asks);
        NewOrderResponse askTail = tail.getValue();
        BigDecimal aot = new BigDecimal(askTail.getPrice());
        BigDecimal newAsk = aot.multiply(gridRate).setScale(pricePrecision, RoundingMode.HALF_EVEN);
        NewOrderResponse newAskOrder =
          this.exchange.limitSell(symbol, TimeInForce.GTC, qtyUnit, newAsk.toPlainString());
        tail.next = new Node(newAskOrder);
        this.asks = asks.next;

        //cancel tail bid order and place new bid order to head
        Node bidTail = getTail(bids);
        boolean success = this.exchange.tryCancelOrder(symbol, bidTail.getValue().getOrderId());
        if (success) {
          removeTail(bids);
          //add new bid order based on existed highest bid price(header) to the head
          NewOrderResponse firstBid = bids.getValue();
          BigDecimal newPrice = new BigDecimal(firstBid.getPrice()).multiply(gridRate)
            .setScale(pricePrecision, RoundingMode.HALF_EVEN);
          NewOrderResponse newOrder =
            this.exchange.limitBuy(symbol, TimeInForce.GTC, qtyUnit, newPrice.toPlainString());
          Node newBid = new Node(newOrder);
          newBid.next = bids;
          bids = newBid;
        }
        log.info("{}, asks: {}", symbol, asks);
      }
      if (node.getNext() != null) {
        checkAskOrderFilled(node.getNext());
      }
    }

    private void removeTail(Node asks) {
      Node prev = null;
      Node n = asks;
      while (n.next != null) {
        prev = n;
        n = n.next;
      }
      if (prev != null) {
        prev.next = null;
      }
    }

    private Node getTail(Node node) {
      while (node.next != null) {
        node = node.next;
      }
      return node;
    }

    private void checkFilledOrder() {
      //check buy orders
      checkBidOrderFilled(bids);
      //check sell orders
      checkAskOrderFilled(asks);
    }

    private void placeOrdersInGrid() {
      log.info("Placing orders for {}", symbol);
      this.pricePrecision = this.exchange.getPricePrecision(symbol);
      BigDecimal iPrice = new BigDecimal(initPrice).setScale(pricePrecision, RoundingMode.HALF_EVEN);

      //place buy orders
      BigDecimal p = new BigDecimal(iPrice.toPlainString());
      p = p.divide(gridRate, RoundingMode.HALF_EVEN).setScale(pricePrecision, RoundingMode.HALF_EVEN);
      NewOrderResponse order = this.exchange.limitBuy(symbol, TimeInForce.GTC, qtyUnit, p.toPlainString());
      this.bids = new Node(order);
      Node tmp = bids;
      for (int i = 0; i < gridSize - 1; i ++) {
        p = p.divide(gridRate, RoundingMode.HALF_EVEN).setScale(pricePrecision, RoundingMode.HALF_EVEN);
        order = this.exchange.limitBuy(symbol, TimeInForce.GTC, qtyUnit, p.toPlainString());
        tmp.next = new Node(order);
        tmp = tmp.next;
      }
      log.info("bid orders: {}", bids);
      //place sell orders
      p = new BigDecimal(iPrice.toPlainString());
      p = p.multiply(gridRate).setScale(pricePrecision, RoundingMode.HALF_EVEN);
      order = this.exchange.limitSell(symbol, TimeInForce.GTC, qtyUnit, p.toPlainString());
      this.asks = new Node(order);
      Node tmp1 = asks;
      for (int i = 0; i < gridSize - 1; i ++) {
        p = p.multiply(gridRate).setScale(pricePrecision, RoundingMode.HALF_EVEN);
        order = this.exchange.limitSell(symbol, TimeInForce.GTC, qtyUnit, p.toPlainString());
        tmp1.next = new Node(order);
        tmp1 = tmp1.next;
      }
      log.info("ask orders: {}", asks);
    }

  }

  @Data
  private static class Node {
    private NewOrderResponse value;
    private Node next;

    Node(NewOrderResponse value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return String.valueOf(value.getOrderId()) +
        ": status: " +
        value.getStatus() +
        ", price: " +
        value.getPrice() + (next == null ? "" : ", " + next.toString());
    }
  }

}
