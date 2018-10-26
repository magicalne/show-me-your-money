package io.magicalne.smym.strategy;

import com.binance.api.client.domain.OrderStatus;
import com.binance.api.client.domain.TimeInForce;
import com.binance.api.client.domain.account.NewOrderResponse;
import com.binance.api.client.domain.account.Order;
import com.binance.api.client.exception.BinanceApiException;
import com.google.common.base.Preconditions;
import io.magicalne.smym.dto.GridTradeConfig;
import io.magicalne.smym.dto.MarketMakingConfig;
import io.magicalne.smym.exchanges.BinanceExchange;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


@Slf4j
public class MarketMakingV1 extends Strategy<MarketMakingConfig> {

  private final BinanceExchange exchange;
  private final List<GridTrading> gridTradings;

  public MarketMakingV1(String accessId, String secretKey, String path) throws IOException {
    this.exchange = new BinanceExchange(accessId, secretKey);
    MarketMakingConfig config = readYaml(path, MarketMakingConfig.class);
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
    Set<String> symbolSet = grids.stream().map(GridTradeConfig::getSymbol).collect(Collectors.toSet());
    this.exchange.createLocalOrderBook(symbolSet, 5);
    return grids.stream().map(g -> new GridTrading(exchange, g)).collect(Collectors.toList());
  }

  private static class GridTrading {
    private static final double COMMISSION = 0.999;

    private final AtomicInteger buys = new AtomicInteger(0);
    private final BinanceExchange exchange;
    private final String symbol;
    private final String qtyUnit;
    private final BigDecimal gridRate;
    private final int gridSize;
    private final int pricePrecision;

    private final LinkedList<NewOrderResponse> bids = new LinkedList<>();
    private final LinkedList<NewOrderResponse> asks = new LinkedList<>();
    private int profit = 0;

    GridTrading(BinanceExchange exchange, GridTradeConfig config) {
      this.exchange = exchange;
      this.symbol = config.getSymbol();
      this.qtyUnit = config.getQtyUnit();
      this.gridRate = new BigDecimal(config.getGridRate());
      this.gridSize = config.getGridSize();
      this.pricePrecision = this.exchange.getPricePrecision(symbol);
    }

    private void checkBidOrderFilled() {
      NewOrderResponse bid = bids.getFirst();
      Order order = this.exchange.queryOrder(symbol, bid.getOrderId());
      if (order.getStatus() == OrderStatus.FILLED) {
        bids.removeFirst();
        buys.incrementAndGet();
        double price = Double.parseDouble(order.getPrice());
        double executedQty = Double.parseDouble(order.getExecutedQty());
        profit -= price * executedQty * COMMISSION;
        log.info("Buy {} - {} - {}, order id: {}, profit: {}", symbol, price, executedQty, order.getOrderId(), profit);
        //place new bid order to tail
        if (this.buys.get() <= 1) {
          NewOrderResponse bidTail = bids.getLast();
          BigDecimal bot = new BigDecimal(bidTail.getPrice());
          BigDecimal newBid = bot.divide(gridRate, RoundingMode.HALF_EVEN)
            .setScale(pricePrecision, RoundingMode.HALF_EVEN);
          try {
            NewOrderResponse newBidOrder =
              this.exchange.limitBuy(symbol, TimeInForce.GTC, qtyUnit, newBid.toPlainString());
            bids.addLast(newBidOrder);
          } catch (BinanceApiException e) {
            log.error("Cannot place bid order due to: ", e);
          }
        }

        //cancel tail ask order and place new ask order to head
        if (!asks.isEmpty()) {
          NewOrderResponse lastAsk = asks.pollLast();
          boolean success = this.exchange.tryCancelOrder(symbol, lastAsk.getOrderId());
          if (success) {
            asks.removeLast();
            //add new ask order based on existed lowest ask price(header) to the head
            NewOrderResponse firstAsk = asks.getFirst();
            BigDecimal newPrice = new BigDecimal(firstAsk.getPrice()).divide(gridRate, RoundingMode.HALF_EVEN)
              .setScale(pricePrecision, RoundingMode.HALF_EVEN);
            try {
              NewOrderResponse newOrder =
                this.exchange.limitSell(symbol, TimeInForce.GTC, qtyUnit, newPrice.toPlainString());
              asks.addFirst(newOrder);
            } catch (BinanceApiException e) {
              log.error("Cannot place ask order due to: ", e);
            }
          }
        }
        logBidsInfo();
      }
    }

    private void checkAskOrderFilled() {
      NewOrderResponse ask = asks.getFirst();
      Order order = this.exchange.queryOrder(symbol, ask.getOrderId());
      if (order.getStatus() == OrderStatus.FILLED) {
        asks.removeFirst();
        buys.decrementAndGet();
        double price = Double.parseDouble(order.getPrice());
        double executedQty = Double.parseDouble(order.getExecutedQty());
        profit += price * executedQty * COMMISSION;
        log.info("Sell {} - {} - {}, order id: {}, profit: {}", symbol, price, executedQty, order.getOrderId(), profit);
        if (this.buys.get() >= -1) {
          //place new ask order to tail
          NewOrderResponse askTail = asks.getLast();
          BigDecimal aot = new BigDecimal(askTail.getPrice());
          BigDecimal newAsk = aot.multiply(gridRate).setScale(pricePrecision, RoundingMode.HALF_EVEN);
          try {
            NewOrderResponse newAskOrder =
              this.exchange.limitSell(symbol, TimeInForce.GTC, qtyUnit, newAsk.toPlainString());
            asks.addLast(newAskOrder);
          } catch (BinanceApiException e) {
            log.error("Cannot place ask order due to: ", e);
          }
        }

        //cancel tail bid order and place new bid order to head
        if (!bids.isEmpty()) {
          NewOrderResponse lastBid = bids.pollLast();
          boolean success = this.exchange.tryCancelOrder(symbol, lastBid.getOrderId());
          if (success) {
            //add new bid order based on existed highest bid price(header) to the head
            NewOrderResponse firstBid = bids.getFirst();
            BigDecimal newPrice = new BigDecimal(firstBid.getPrice()).multiply(gridRate)
              .setScale(pricePrecision, RoundingMode.HALF_EVEN);
            try {
              NewOrderResponse newOrder =
                this.exchange.limitBuy(symbol, TimeInForce.GTC, qtyUnit, newPrice.toPlainString());
              bids.addFirst(newOrder);
            } catch (BinanceApiException e) {
              log.error("Cannot place order due to: ", e);
            }
          }
        }
        logAsksInfo();
      }
    }

    private void checkFilledOrder() {
      if (bids.isEmpty() && asks.isEmpty()) {
        placeOrdersInGrid();
      } else {
        if (!bids.isEmpty()) {
          checkBidOrderFilled();
        }
        if (!asks.isEmpty()) {
          checkAskOrderFilled();
        }
      }
    }

    private void placeOrdersInGrid() {
      log.info("Placing orders for {}", symbol);
      double mp = this.exchange.getMidPriceFromOrderBook(symbol);
      if (mp < 0) {
        throw new RuntimeException("Middle price of " + symbol + " is " + mp);
      }
      placeBidOrders(mp);
      placeAskOrders(mp);

      log.info("Place bid orders:");
      logBidsInfo();
      log.info("Place ask orders:");
      logAsksInfo();
    }

    private void logAsksInfo() {
      asks.forEach(ask ->
        log.info("order id: {}, status: {}, price: {}", ask.getOrderId(), ask.getStatus(), ask.getPrice()));
    }

    private void logBidsInfo() {
      bids.forEach(bid ->
        log.info("order id: {}, status: {}, price: {}", bid.getOrderId(), bid.getStatus(), bid.getPrice()));
    }

    private void placeAskOrders(double midPrice) {
      BigDecimal p = new BigDecimal(midPrice).setScale(pricePrecision, RoundingMode.HALF_EVEN);
      p = p.multiply(gridRate).setScale(pricePrecision, RoundingMode.HALF_EVEN);
      NewOrderResponse order = this.exchange.limitSell(symbol, TimeInForce.GTC, qtyUnit, p.toPlainString());
      asks.add(order);
      for (int i = 0; i < gridSize - 1; i ++) {
        p = p.multiply(gridRate).setScale(pricePrecision, RoundingMode.HALF_EVEN);
        order = this.exchange.limitSell(symbol, TimeInForce.GTC, qtyUnit, p.toPlainString());
        asks.add(order);
      }
    }

    private void placeBidOrders(double midPrice) {
      //place buy orders
      BigDecimal p = new BigDecimal(midPrice).setScale(pricePrecision, RoundingMode.HALF_EVEN);
      p = p.divide(gridRate, RoundingMode.HALF_EVEN).setScale(pricePrecision, RoundingMode.HALF_EVEN);
      NewOrderResponse order = this.exchange.limitBuy(symbol, TimeInForce.GTC, qtyUnit, p.toPlainString());
      bids.add(order);
      for (int i = 0; i < gridSize - 1; i ++) {
        p = p.divide(gridRate, RoundingMode.HALF_EVEN).setScale(pricePrecision, RoundingMode.HALF_EVEN);
        order = this.exchange.limitBuy(symbol, TimeInForce.GTC, qtyUnit, p.toPlainString());
        bids.add(order);
      }
    }
  }
}
