package io.magicalne.smym.strategy;

import com.binance.api.client.domain.OrderStatus;
import com.binance.api.client.domain.TimeInForce;
import com.binance.api.client.domain.account.NewOrderResponse;
import com.binance.api.client.domain.account.Order;
import com.binance.api.client.domain.market.OrderBookEntry;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.magicalne.smym.dto.Triangle;
import io.magicalne.smym.dto.TriangleArbitrageConfig;
import io.magicalne.smym.exchanges.BinanceExchange;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class BinanceTriangleArbitrageV2 extends Strategy<TriangleArbitrageConfig> {

  private final BinanceExchange exchange;

  public BinanceTriangleArbitrageV2(String accessId, String secretKey, String path)
    throws IOException, InterruptedException {
    this.exchange = new BinanceExchange(accessId, secretKey);
    TriangleArbitrageConfig config = readYaml(path, TriangleArbitrageConfig.class);
    init(config);
  }

  private void init(TriangleArbitrageConfig config) throws InterruptedException {
    List<Executor> executors = new LinkedList<>();
    for (Triangle triangle : config.getTriangles()) {
      Executor executor = new Executor(triangle, exchange);
      executors.add(executor);
    }

    for (;;) {
      for (Executor executor : executors) {
        try {
          executor.checkOrderStatus();
        } catch (InterruptedException | ExecutionException e) {
          log.error("Something bad happened...", e);
          return;
        }
      }
      Thread.sleep(10000);
    }
  }

  @Slf4j
  private static class Executor {
    private final BinanceExchange exchange;
    private static final double COMMSSION = Math.pow(0.999, 3);
    private final double priceRate;
    private final String startQty;
    private final String startSymbol;
    private final String middleSymbol;
    private final String lastSymbol;
    private final int startSymbolPricePrecision;
    private final int startSymbolQtyPrecision;
    private final int middleSymbolPricePrecision;
    private final int middleSymbolQtyPrecision;
    private final int lastSymbolPricePrecision;
    private final int lastSymbolQtyPrecision;
    private final ExecutorService executorService;

    private final AtomicInteger cnt = new AtomicInteger(0);

    private List<NewOrderResponse> orders = null;

    private Executor(Triangle triangle, BinanceExchange exchange) {
      this.priceRate = triangle.getPriceRate();
      this.startQty = triangle.getStartQty();
      this.startSymbol = triangle.getStartSymbol();
      this.middleSymbol = triangle.getMiddleSymbol();
      this.lastSymbol = triangle.getLastSymbol();
      this.exchange = exchange;
      this.startSymbolPricePrecision = this.exchange.getPricePrecision(startSymbol);
      this.startSymbolQtyPrecision = this.exchange.getQtyPrecision(startSymbol);
      this.middleSymbolPricePrecision = this.exchange.getPricePrecision(middleSymbol);
      this.middleSymbolQtyPrecision = this.exchange.getQtyPrecision(middleSymbol);
      this.lastSymbolPricePrecision = this.exchange.getPricePrecision(lastSymbol);
      this.lastSymbolQtyPrecision = this.exchange.getQtyPrecision(lastSymbol);
      HashSet<String> symbols =
        Sets.newHashSet(triangle.getStartSymbol(), triangle.getMiddleSymbol(), triangle.getLastSymbol());
      this.exchange.createLocalOrderBook(symbols, 5);

      executorService =
        Executors.newFixedThreadPool(3, new ThreadFactoryBuilder().setNameFormat("TA-thread-%d").build());
    }

    private void placeOrders() throws InterruptedException, ExecutionException {
      cnt.set(0);
      OrderBookEntry sobe = exchange.getBestBid(startSymbol);
      double sp = Double.parseDouble(sobe.getPrice());
      OrderBookEntry mobe = exchange.getBestBid(middleSymbol);
      double mp = Double.parseDouble(mobe.getPrice());
      OrderBookEntry lobe = exchange.getBestAsk(lastSymbol);
      double lp = Double.parseDouble(lobe.getPrice());
      if (findArbitrage(sp, mp, lp)) {
        log.info("Find arbitrage space.");
        List<Callable<NewOrderResponse>> calls = new LinkedList<>();
        BigDecimal sQty = new BigDecimal(startQty).setScale(startSymbolQtyPrecision, RoundingMode.HALF_EVEN);
        BigDecimal spbd = new BigDecimal(sp).setScale(startSymbolPricePrecision, RoundingMode.HALF_EVEN);
        calls.add(() ->
          exchange.limitBuy(startSymbol, TimeInForce.GTC, sQty.toPlainString(), spbd.toPlainString()));

        BigDecimal mpbd = new BigDecimal(mp).setScale(middleSymbolPricePrecision, RoundingMode.HALF_EVEN);
        BigDecimal mQty = sQty.divide(mpbd, middleSymbolQtyPrecision, RoundingMode.HALF_EVEN);
        calls.add(() ->
          exchange.limitBuy(middleSymbol, TimeInForce.GTC, mQty.toPlainString(), mpbd.toPlainString()));

        BigDecimal lpbd = new BigDecimal(lp).setScale(lastSymbolPricePrecision, RoundingMode.HALF_EVEN);
        calls.add(() ->
          exchange.limitSell(lastSymbol, TimeInForce.GTC, mQty.toPlainString(), lpbd.toPlainString()));

        List<Future<NewOrderResponse>> futures = executorService.invokeAll(calls);
        List<NewOrderResponse> orderIdList = new LinkedList<>();
        log.info("Placing orders...");
        for (Future<NewOrderResponse> f : futures) {
          NewOrderResponse res = f.get();
          log.info("Place order: {} - {} - {}, {}",
            res.getSymbol(), res.getOrderId(), res.getStatus(), res.getTransactTime());
          orderIdList.add(res);
        }
        this.orders = orderIdList;
      } else {
        placeOrders();
      }
    }

    private void checkOrderStatus() throws ExecutionException, InterruptedException {
      if (this.orders == null || this.orders.isEmpty()) {
        placeOrders();
      } else {
        List<Order> queryOrders = new LinkedList<>();
        int filled = 0;
        int submitted = 0;
        for (NewOrderResponse res : orders) {
          Order order = this.exchange.queryOrder(res.getSymbol(), res.getOrderId());
          if (order.getStatus() == OrderStatus.FILLED) {
            log.info("{} was filled.", order.getSymbol());
            filled ++;
          } else if (order.getStatus() == OrderStatus.NEW) {
            submitted ++;
          } else if (order.getStatus() == OrderStatus.PARTIALLY_FILLED) {
            log.info("{} was partially filled.", order.getSymbol());
          }
          queryOrders.add(order);
        }
        if (filled == 3) {
          log.info("All were filled!!! Show me your money, baby!!!");
          calculateProfit(queryOrders);
          placeOrders();
        }

        if (submitted == 3) {
          log.info("No filled order yet.");
          cnt.incrementAndGet();
          if (cnt.get() == 1) {
            cancelOrders(queryOrders);
            placeOrders();
          }
        }
      }
    }

    private void cancelOrders(List<Order> queryOrders) {
      queryOrders.forEach(o -> this.exchange.cancelOrder(o.getSymbol(), o.getOrderId()));
      log.info("Cancel old orders");
    }

    private void calculateProfit(List<Order> queryOrders) {
      Order startOrder = queryOrders.get(0);
      double sp = Double.parseDouble(startOrder.getPrice());
      double sq = Double.parseDouble(startOrder.getExecutedQty());
      double sBase = sp * sq;

      Order lastOrder = queryOrders.get(2);
      double lp = Double.parseDouble(lastOrder.getPrice());
      double lq = Double.parseDouble(lastOrder.getExecutedQty());
      double lBase = lp * lq;
      log.info("Profit from {} -> {} -> {} is {}",
        startSymbol, middleSymbol, lastSymbol, lBase - sBase);
    }

    private boolean findArbitrage(double sp, double mp, double lp) {
      return lp*COMMSSION/mp/sp > priceRate;
    }
  }
}
