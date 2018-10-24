package io.magicalne.smym.strategy;

import com.binance.api.client.domain.market.OrderBookEntry;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.magicalne.smym.dto.Triangle;
import io.magicalne.smym.dto.TriangleArbitrageConfig;
import io.magicalne.smym.exchanges.BinanceExchange;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BinanceTriangleArbitrageV2 extends Strategy<TriangleArbitrageConfig> {

  private final BinanceExchange exchange;

  public BinanceTriangleArbitrageV2(String accessId, String secretKey, String path) throws IOException {
    this.exchange = new BinanceExchange(accessId, secretKey);
    TriangleArbitrageConfig config = readYaml(path, TriangleArbitrageConfig.class);
    init(config);
  }

  private void init(TriangleArbitrageConfig config) {
    List<Executor> executors = new LinkedList<>();
    for (Triangle triangle : config.getTriangles()) {
      Executor executor = new Executor(triangle, exchange);
      executors.add(executor);
    }

    for (;;) {
      for (Executor executor : executors) {
        executor.check();
      }
    }
  }

  @Slf4j
  private static class Executor {
    private final BinanceExchange exchange;
    private static final double COMMSSION = Math.pow(0.999, 3);
    private final double priceRate;
    private final String startBaseQty;
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

    private Executor(Triangle triangle, BinanceExchange exchange) {
      this.priceRate = triangle.getPriceRate();
      this.startBaseQty = triangle.getStartBaseQty();
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

    private void placeOrders() {
      OrderBookEntry sobe = exchange.getBestBid(startSymbol);
      double sp = Double.parseDouble(sobe.getPrice()) / priceRate;
      OrderBookEntry mobe = exchange.getBestBid(middleSymbol);
      double mp = Double.parseDouble(mobe.getPrice()) / priceRate;
      OrderBookEntry lobe = exchange.getBestAsk(lastSymbol);
      double lp = Double.parseDouble(lobe.getPrice()) * priceRate;
      if (findArbitrage(sp, mp, lp)) {
        log.info("Find arbitrage space.");
        executorService.

      } else {
        placeOrders();
      }
    }

    private boolean findArbitrage(double sp, double mp, double lp) {
      return lp/sp*sp*COMMSSION > priceRate;
    }
  }
}
