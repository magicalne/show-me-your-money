package io.magicalne.smym.strategy;

import com.google.common.base.Preconditions;
import com.google.common.math.Stats;
import io.magicalne.smym.dto.bitmex.AlgoTrading;
import io.magicalne.smym.dto.bitmex.BitmexConfig;
import io.magicalne.smym.exchanges.bitmex.BitmexDeltaClient;
import io.magicalne.smym.exchanges.bitmex.BitmexExchange;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.knowm.xchange.bitmex.dto.marketdata.BitmexKline;
import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.exceptions.RateLimitExceededException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class BitmexArbitrage extends Strategy<BitmexConfig> {

  private final BitmexExchange exchange;
  private final BitmexConfig config;

  public BitmexArbitrage(String path)
    throws IOException {
    String accessKey = System.getenv("BITMEX_ACCESS_KEY");
    String secretKey = System.getenv("BITMEX_ACCESS_SECRET_KEY");
    this.exchange = new BitmexExchange(accessKey, secretKey);
    this.config = readYaml(path, BitmexConfig.class);
  }

  public void execute() throws InterruptedException {
    List<AlgoTrading> algoTradings = config.getAlgoTradings();
    List<Arbitrage> list = new LinkedList<>();
    for (AlgoTrading a : algoTradings) {
      Arbitrage afp = new Arbitrage(config.getDeltaHost(), config.getDeltaPort(), a, exchange);
      afp.setup();
      list.add(afp);
    }

    for (; ; ) {
      for (Arbitrage ofp : list) {
        try {
          ofp.execute();
        } catch (RateLimitExceededException e) {
          log.warn("Need to retry in second due to: ", e);
          Thread.sleep(1500);
        } catch (ExchangeException e) {
          log.error("Bitmex exehange exception: ", e);
          Thread.sleep(500);
        } catch (Exception e) {
          log.error("Trading with exception: ", e);
        }
      }
    }
  }

  @Slf4j
  public static class Arbitrage {

    private static final double FEE = 0.00075;
    private static final double REBATE = 0.00025;
    private static final double TICK = 0.5;
    private static final double IMBALANCE = 0.3;
    private final double spread;
    private final BitmexDeltaClient deltaClient;
    private final String symbol;
    private final int contract;
    private final double leverage;
    private final BitmexExchange exchange;

    private final String swap = "XBTUSD";
    private final String future = "XBTH19";

    private short trading = 0;

    private double mean;
    private double std;
    private double shortSwap;
    private double longSwap;
    private double shortFuture;
    private double longFuture;
    private double profit = 0;

    Arbitrage(String deltaHost, int deltaPort, AlgoTrading config, BitmexExchange exchange) {
      this.deltaClient = new BitmexDeltaClient(deltaHost, deltaPort);
      this.symbol = config.getSymbol();
      this.contract = config.getContract();
      this.leverage = config.getLeverage();
      this.spread = config.getSpread();
      this.exchange = exchange;
    }

    private void setup() {
//      exchange.setLeverage(symbol, leverage);
      List<BitmexKline> swapKline = exchange.getRecentStats(swap);
      List<BitmexKline> futureKline = exchange.getRecentStats(future);
      Preconditions.checkState(swapKline.size() == futureKline.size());
      List<Double> diff = new LinkedList<>();
      for (int i = 0; i < swapKline.size(); i++) {
        BitmexKline s = swapKline.get(i);
        BitmexKline f = futureKline.get(i);
        diff.add(f.getClose().subtract(s.getClose()).doubleValue());
      }
      Stats stats = Stats.of(diff);
      mean = stats.mean();
      std = stats.populationStandardDeviation();
      log.info("For the last 10 days, mean: {}, std: {}.", mean, std);
    }

    private void execute() throws IOException {
      test();
    }

    private void test() throws IOException {
      BitmexDeltaClient.OrderBookL2 swapOB = deltaClient.getOrderBookL2(swap);
      double swapMid = swapOB.bestMid();
      BitmexDeltaClient.OrderBookL2 futureOB = deltaClient.getOrderBookL2(future);
      double futureMid = futureOB.bestMid();
      double basis = futureMid - swapMid;
      double longSwap = swapOB.getBestAsk().getPrice();
      double shortSwap = swapOB.getBestBid().getPrice();
      double longFuture = futureOB.getBestAsk().getPrice();
      double shortFuture = futureOB.getBestBid().getPrice();
      if (trading == 0) {
        if (basis < mean - std) {
          this.shortSwap = shortSwap;
          this.longFuture = longFuture;
          trading = -1;
          log.info("short swap: {}, long future: {}", this.shortSwap, this.longFuture);
        } else if (basis > mean + std) {
          this.longSwap = longSwap;
          this.shortFuture = shortFuture;
          trading = 1;
          log.info("long swap: {}, short future: {}", this.longSwap, this.shortFuture);
        }
      } else if (trading > 0) {
        double p = profit(this.longFuture, shortFuture, longSwap, this.shortSwap);
        if (p > 0 && basis > mean - std) {
          log.info("long swap: {}, short future: {}", longSwap, shortFuture);
          this.profit += p;
          log.info("Profit of this round: {}, total profit: {}", p, profit);
          trading = 0;
        }
      } else {
        double p = profit(longFuture, this.shortFuture, this.longSwap, shortSwap);
        if (p > 0 && basis < mean + std) {
          log.info("short swap: {}, long future: {}", shortSwap, longFuture);
          this.profit += p;
          log.info("Profit of this round: {}, total profit: {}", p, profit);
          trading = 0;
        }
      }
    }

    private double profit(double longFuture, double shortFuture, double longSwap, double shortSwap) {
      return contract * (1/longFuture - 1/shortFuture + 1/longSwap - 1/shortSwap)
        - FEE * contract * (1/longFuture + 1/shortFuture + 1/longSwap + 1/shortSwap);
    }

    @Data
    private static class Position {
      private double price;
      private int contract;

      Position(double price, int contract) {
        this.price = price;
        this.contract = contract;
      }

      void update(double p, int c) {
        this.price = p;
        this.contract = c;
      }
    }
  }
}
