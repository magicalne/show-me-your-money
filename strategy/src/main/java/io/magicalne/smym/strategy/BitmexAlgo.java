package io.magicalne.smym.strategy;

import com.google.common.annotations.VisibleForTesting;
import io.magicalne.smym.dto.bitmex.AlgoTrading;
import io.magicalne.smym.dto.bitmex.BitmexConfig;
import io.magicalne.smym.exchanges.bitmex.BitmexDeltaClient;
import io.magicalne.smym.exchanges.bitmex.BitmexExchange;
import io.magicalne.smym.exchanges.bitmex.BitmexQueryOrderException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.model.PMMLUtil;
import org.knowm.xchange.bitmex.dto.marketdata.BitmexPrivateOrder;
import org.knowm.xchange.bitmex.dto.trade.*;
import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.exceptions.RateLimitExceededException;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Slf4j
public class BitmexAlgo extends Strategy<BitmexConfig> {

  private final BitmexExchange exchange;
  private final BitmexConfig config;

  public BitmexAlgo(String path)
    throws IOException {
    String accessKey = System.getenv("BITMEX_ACCESS_KEY");
    String secretKey = System.getenv("BITMEX_ACCESS_SECRET_KEY");
    this.exchange = new BitmexExchange(accessKey, secretKey);
    this.config = readYaml(path, BitmexConfig.class);
  }

  public void execute() throws InterruptedException {
    List<AlgoTrading> algoTradings = config.getAlgoTradings();
    List<MarketMaker> list = new LinkedList<>();
    for (AlgoTrading a : algoTradings) {
      MarketMaker afp = new MarketMaker(config.getDeltaHost(), config.getDeltaPort(), a, exchange);
      afp.setup();
      list.add(afp);
    }

    for (; ; ) {
      for (MarketMaker ofp : list) {
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
  public static class MarketMaker {

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
    private final int limit;

    private final LinkedList<BitmexPrivateOrder> bids;
    private final LinkedList<BitmexPrivateOrder> asks;

    private BitmexPrivateOrder bid;
    private BitmexPrivateOrder ask;
    private Position position = null;
    private double profit = 0;
    private final double stopLoss = 0.1;

    MarketMaker(String deltaHost, int deltaPort, AlgoTrading config, BitmexExchange exchange) {
      this.deltaClient = new BitmexDeltaClient(deltaHost, deltaPort);
      this.symbol = config.getSymbol();
      this.contract = config.getContract();
      this.leverage = config.getLeverage();
      this.spread = config.getSpread();
      this.limit = config.getLimit();
      this.exchange = exchange;
      bids = new LinkedList<>();
      asks = new LinkedList<>();
    }

    private Evaluator initPMML(String pmmlPath) throws IOException, JAXBException, SAXException {
      Path path = Paths.get(pmmlPath);
      try (InputStream is = Files.newInputStream(path)) {
        PMML pmml = PMMLUtil.unmarshal(is);
        ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
        return modelEvaluatorFactory.newModelEvaluator(pmml);
      }
    }

    private void setup() {
      exchange.setLeverage(symbol, leverage);
    }

    private void execute() throws IOException {
      try {
        placeOrders();
      } catch (BitmexQueryOrderException ignore) {

      }
    }

    private void placeOrders() throws IOException, BitmexQueryOrderException {
      BitmexDeltaClient.OrderBookL2 ob = deltaClient.getOrderBookL2(symbol);
      double bestBid = ob.getBestBid().getPrice();
      double bestAsk = ob.getBestAsk().getPrice();
      double mid = (bestBid + bestAsk) / 2;
      double skew = mid * (spread / 2);
      if (bid == null && ask == null) {
        if (position == null) {
          long bidPrice = Math.round(bestBid * (1 - spread));
          long askPrice = Math.round(bestAsk * (1 + spread));
          placePairOrder(bidPrice, askPrice);
        } else if (Math.min(bids.size(), asks.size()) + position.getContract()/contract < limit) {
          if (position.getContract() > 0) {
            double askPrice;
            if (position.getPrice() > mid) {
              askPrice = Math.round(position.getPrice()) + TICK;
            } else {
              askPrice = Math.round(bestAsk * (1 + spread) - skew);
            }
            long bidPrice = Math.round(bestBid * (1 - spread) - skew * (position.getContract() / contract));
            placePairOrder(bidPrice, askPrice);
          } else if (position.getContract() < 0) {
            double bidPrice;
            if (-position.getPrice() > mid) {
              bidPrice = Math.round(bestBid * (1 - spread) + skew);
            } else {
              bidPrice = Math.round(-position.getPrice()) - TICK;
            }
            long askPrice = Math.round(bestAsk * (1 + spread) + skew * (position.getContract() / contract));
            placePairOrder(bidPrice, askPrice);
          }
        }
      } else {
        checkBidOrder(bestAsk);
        checkAskOrder(bestBid);
        if (!bids.isEmpty()) {
          BitmexPrivateOrder bid = deltaClient.getOrderById(symbol, bids.getLast().getId());
          if (bid.getOrderStatus() == BitmexPrivateOrder.OrderStatus.Filled) {
            profit += contract / bid.getPrice().doubleValue() + REBATE * contract / bid.getPrice().doubleValue();
            bids.removeLast();
            log.info("Bid filled at {}", bid);
            if (position == null) {
              position = new Position(bid.getPrice().doubleValue(), contract);
            } else {
              if (position.getPrice() > 0) {
                int c = contract + position.getContract();
                double p = c / (contract / bid.getPrice().doubleValue() + position.getContract() / position.getPrice());
                position.update(p, c);
              } else {
                if (position.getContract() == contract) {
                  position = null;
                  log.info("Liquidate profit: {}", profit);
                } else {
                  double newPos = (position.getContract() - contract) / (position.getContract() / position.getPrice() + contract / bid.getPrice().doubleValue());
                  position.update(newPos, position.getContract() - contract);
                }
              }
            }
            log.info("Position: {}", position);
          } else if (bid.getOrderStatus() == BitmexPrivateOrder.OrderStatus.Canceled) {
            bid = exchange.placeLimitOrder(symbol, Math.min(bid.getPrice().doubleValue(), bestBid), contract, BitmexSide.BUY);
            bids.removeLast();
            addBid(bid);
            log.info("Replace bid order due to cancel. {}", bid.getId());
          }
        }

        if (!asks.isEmpty()) {
          BitmexPrivateOrder ask = deltaClient.getOrderById(symbol, asks.getFirst().getId());
          if (ask.getOrderStatus() == BitmexPrivateOrder.OrderStatus.Filled) {
            profit += -contract / ask.getPrice().doubleValue() + REBATE * contract / ask.getPrice().doubleValue();
            asks.removeFirst();
            log.info("Ask filled at {}", ask);
            if (position == null) {
              position = new Position(-ask.getPrice().doubleValue(), contract);
            } else {
              if (position.getPrice() < 0) {
                int c = contract + position.getContract();
                double p = c / (contract / ask.getPrice().doubleValue() - position.getContract() / position.getPrice());
                position.update(-p, c);
              } else {
                if (position.getContract() == contract) {
                  position = null;
                  log.info("Liquidate profit: {}", profit);
                } else {
                  double newPos = (position.getContract() - contract) / (position.getContract() / position.getPrice() - contract / ask.getPrice().doubleValue());
                  position.update(newPos, position.getContract() - contract);
                }
              }
            }
            log.info("Position: {}", position);
          } else if (ask.getOrderStatus() == BitmexPrivateOrder.OrderStatus.Canceled) {
            ask = exchange.placeLimitOrder(symbol, Math.max(ask.getPrice().doubleValue(), bestAsk), contract, BitmexSide.SELL);
            asks.removeLast();
            addAsk(ask);
            log.info("Replace ask order due to cancel. {}", ask.getId());
          }
        }
      }
      stopLoss(bestBid, bestAsk);
    }

    private void addBid(BitmexPrivateOrder bid) {
      bids.add(bid);
      if (bids.size() > 1) {
        bids.sort(Comparator.comparing(BitmexPrivateOrder::getPrice));
      }
    }

    private void addAsk(BitmexPrivateOrder ask) {
      asks.add(ask);
      if (asks.size() > 1) {
        asks.sort(Comparator.comparing(BitmexPrivateOrder::getPrice));
      }
    }

    private void checkBidOrder(double bestAsk) throws BitmexQueryOrderException, IOException {
      if (this.bid != null) {
        bid = deltaClient.getOrderById(symbol, this.bid.getId());
        if (bid.getOrderStatus() == BitmexPrivateOrder.OrderStatus.Filled) {
          ask = exchange.amendOrderPrice(ask.getId(), contract, Math.max(bid.getPrice().doubleValue()+TICK, bestAsk));
          log.info("Amend ask order and put into queue. {}", ask);
          addAsk(ask);
          addBid(bid);
          bid = null;
          ask = null;
        }
      }
    }

    private void checkAskOrder(double bestBid) throws BitmexQueryOrderException, IOException {
      if (this.ask != null) {
        ask = deltaClient.getOrderById(symbol, this.ask.getId());
        if (ask.getOrderStatus() == BitmexPrivateOrder.OrderStatus.Filled) {
          bid = exchange.amendOrderPrice(bid.getId(), contract, Math.min(ask.getPrice().doubleValue()-TICK, bestBid));
          log.info("Amend bid order and put into queue. {}", bid);
          addBid(bid);
          addAsk(ask);
          bid = null;
          ask = null;
        }
      }
    }

    private void placePairOrder(double bidPrice, double askPrice) {
      List<BitmexPrivateOrder> orders = exchange.placePairOrders(symbol, bidPrice, askPrice, contract);
      bid = orders.get(0);
      ask = orders.get(1);
      log.info("Place bid at {}, ask: {}", bidPrice, askPrice);
    }

    private double getSkew(int change, int total, double spread) {
      return (1.0d * change / total) * spread * -1;
    }

    private double calculateLeverage(double tradePrice, double newPrice) {
      final int scale = 2;
      RoundingMode mode = Double.compare(tradePrice, newPrice) > 0 ? RoundingMode.UP : RoundingMode.DOWN;
      return new BigDecimal(tradePrice * leverage)
        .divide(new BigDecimal(newPrice), scale, mode)
        .doubleValue();
    }

    private double getRoundPrice(double price, double spreed) {
      double roundPrice = price * spreed;
      double round = Math.round(roundPrice);
      if (roundPrice < round) {
        roundPrice = round;
      } else {
        roundPrice = round + TICK;
      }
      return roundPrice;
    }

    private List<BitmexPrivateOrder> marketAndLimit(BitmexSide marketSide, BitmexSide limitSide, double limitPrice) {
      BigDecimal orderQuantity = new BigDecimal(contract);
      BitmexPlaceOrderParameters market = new BitmexPlaceOrderParameters.Builder(symbol)
        .setSide(marketSide)
        .setOrderType(BitmexOrderType.MARKET)
        .setOrderQuantity(orderQuantity)
        .build();
      BitmexPlaceOrderParameters limit = new BitmexPlaceOrderParameters.Builder(symbol)
        .setSide(limitSide)
        .setPrice(new BigDecimal(limitPrice))
        .setOrderType(BitmexOrderType.LIMIT)
        .setOrderQuantity(orderQuantity)
        .setExecutionInstructions(Collections.singletonList(BitmexExecutionInstruction.PARTICIPATE_DO_NOT_INITIATE))
        .build();

      PlaceOrderCommand m = new PlaceOrderCommand(market);
      PlaceOrderCommand l = new PlaceOrderCommand(limit);
      return exchange.placeOrdersBulk(Arrays.asList(m, l));
    }

    private void stopLoss(double bestBid, double bestAsk) throws IOException, BitmexQueryOrderException {
      if (!bids.isEmpty()) {
        BitmexPrivateOrder bid = deltaClient.getOrderById(symbol, bids.getFirst().getId());
        double price = bid.getPrice().doubleValue();
        if ((bestBid - price) / bestBid > stopLoss && position != null && position.getPrice() < 0) {
          log.info("Stop loss from: {}", bid.getPrice());
          if (bid.getOrderStatus() == BitmexPrivateOrder.OrderStatus.New) {
            exchange.amendOrderPrice(bid.getId(), contract, bestBid);
          } else if (bid.getOrderStatus() == BitmexPrivateOrder.OrderStatus.Canceled) {
            bids.removeFirst();
            BitmexPrivateOrder newBid = exchange.placeLimitOrder(symbol, bestBid, contract, BitmexSide.BUY);
            addBid(newBid);
          }
        }
      }

      if (!asks.isEmpty()) {
        BitmexPrivateOrder ask = deltaClient.getOrderById(symbol, asks.getLast().getId());
        double price = ask.getPrice().doubleValue();
        if ((price - bestAsk) / price > stopLoss && position != null && position.getPrice() > 0) {
          log.info("Stop loss from: {}", ask.getPrice());
          if (ask.getOrderStatus() == BitmexPrivateOrder.OrderStatus.New) {
            exchange.amendOrderPrice(ask.getId(), contract, bestAsk);
          } else if (ask.getOrderStatus() == BitmexPrivateOrder.OrderStatus.Canceled) {
            asks.removeLast();
            BitmexPrivateOrder newAsk = exchange.placeLimitOrder(symbol, bestAsk, contract, BitmexSide.SELL);
            addAsk(newAsk);
          }
        }
      }
    }

    private BitmexPrivateOrder tryAmendLongOrder(String orderId, double price, boolean longCanceled) {
      return longCanceled ? exchange.placeLimitOrder(symbol, price, contract, BitmexSide.BUY) :
        exchange.amendOrderPrice(orderId, contract, price);
    }

    private BitmexPrivateOrder tryAmendShortOrder(String orderId, double price, boolean shortCanceled) {
      return shortCanceled ? exchange.placeLimitOrder(symbol, price, contract, BitmexSide.SELL) :
        exchange.amendOrderPrice(orderId, contract, price);
    }

    @VisibleForTesting
    public static Map<String, Double> extractFeature(Queue<BitmexDeltaClient.OrderBookL2> queue) {
      List<Map<String, Double>> rows = new LinkedList<>();
      for (BitmexDeltaClient.OrderBookL2 orderBookL2 : queue) {
        List<BitmexDeltaClient.OrderBookEntry> asks = orderBookL2.getAsks();
        List<BitmexDeltaClient.OrderBookEntry> bids = orderBookL2.getBids();
        Map<String, Double> featureMap = new HashMap<>();
        double askPriceSum = 0;
        double bidPriceSum = 0;
        double askVolSum = 0;
        double bidVolSum = 0;
        double spreedVolSum = 0;
        double spreedSum = 0;
        for (int i = 0; i < 10; i++) {
          BitmexDeltaClient.OrderBookEntry ask = asks.get(i);
          BitmexDeltaClient.OrderBookEntry bid = bids.get(i);
          featureMap.put("ask_p_" + i, ask.getPrice());
          featureMap.put("ask_vol_" + i, (double) ask.getSize());
          featureMap.put("bid_p_" + i, bid.getPrice());
          featureMap.put("bid_vol_" + i, (double) bid.getSize());
          double spreed = ask.getPrice() - bid.getPrice();
          spreedSum += spreed;
          featureMap.put("spreed_" + i, spreed);
          featureMap.put("mid_p_" + i, (ask.getPrice() + bid.getPrice()) / 2);
          double spreedVol = ask.getSize() - bid.getSize();
          featureMap.put("spreed_vol_" + i, spreedVol);
          spreedVolSum += spreedVol;
          featureMap.put("vol_rate_" + i, ask.getSize() * 1.0d / bid.getSize());
          featureMap.put("sask_vol_rate_" + i, spreedVol / ask.getSize());
          featureMap.put("sbid_vol_rate_" + i, spreedVol / bid.getSize());
          double volSum = ask.getSize() + bid.getSize();
          featureMap.put("ask_vol_rate_" + i, ask.getSize() / volSum);
          featureMap.put("bid_vol_rate_" + i, bid.getSize() / volSum);
          askPriceSum += ask.getPrice();
          bidPriceSum += bid.getPrice();
          askVolSum += ask.getSize();
          bidVolSum += bid.getSize();
        }
        for (int i = 0; i < 9; i++) {
          int k = i + 1;
          featureMap.put("ask_p_diff_" + k + "_0", featureMap.get("ask_p_" + k) - featureMap.get("ask_p_0"));
          featureMap.put("bid_p_diff_" + k + "_0", featureMap.get("bid_p_" + k) - featureMap.get("bid_p_0"));
          featureMap.put("ask_p_diff_" + k + "_" + i, featureMap.get("ask_p_" + k) - featureMap.get("ask_p_" + i));
          featureMap.put("bid_p_diff_" + k + "_" + i, featureMap.get("bid_p_" + k) - featureMap.get("bid_p_" + i));
        }
        featureMap.put("ask_p_mean", askPriceSum / 10);
        featureMap.put("bid_p_mean", bidPriceSum / 10);
        featureMap.put("ask_vol_mean", askVolSum / 10);
        featureMap.put("bid_vol_mean", bidVolSum / 10);
        featureMap.put("accum_spreed_vol", spreedVolSum);
        featureMap.put("accum_spreed", spreedSum);
        rows.add(featureMap);
      }
      Map<String, Double> featureMap = rows.get(9);
      List<Double> askPrices = new LinkedList<>();
      List<Double> bidPrices = new LinkedList<>();
      List<Double> askVols = new LinkedList<>();
      List<Double> bidVols = new LinkedList<>();
      for (Map<String, Double> row : rows) {
        askPrices.add(row.get("ask_p_0"));
        bidPrices.add(row.get("bid_p_0"));
        askVols.add(row.get("ask_vol_0"));
        bidVols.add(row.get("bid_vol_0"));
      }
      for (int i = 9; i >= 2; i--) {
        askPrices.remove(0);
        extractTSFeature(featureMap, askPrices, "ask_p_roll_" + i);
        bidPrices.remove(0);
        extractTSFeature(featureMap, bidPrices, "bid_p_roll_" + i);
        askVols.remove(0);
        extractTSFeature(featureMap, askVols, "ask_v_roll_" + i);
        bidVols.remove(0);
        extractTSFeature(featureMap, bidVols, "bid_v_roll_" + i);
      }
      return featureMap;
    }

    private static void extractTSFeature(Map<String, Double> featureMap, List<Double> askPrices, String prefix) {
      Stats askPriceStats = timeSeriesFeature(askPrices);
      featureMap.put(prefix + "_mean", askPriceStats.getMean());
      featureMap.put(prefix + "_std", askPriceStats.getStd());
      featureMap.put(prefix + "_var", askPriceStats.getVar());
      featureMap.put(prefix + "_skew", askPriceStats.getSkew());
      featureMap.put(prefix + "_kurt", askPriceStats.getKurt());
    }

    private static Stats timeSeriesFeature(List<Double> list) {
      DescriptiveStatistics stats = new DescriptiveStatistics();
      for (double e : list) {
        stats.addValue(e);
      }
      return new Stats(
        stats.getMean(), stats.getStandardDeviation(), stats.getVariance(), stats.getSkewness(), stats.getKurtosis());
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

    @Data
    private static class Stats {
      private double mean;
      private double std;
      private double var;
      private double skew;
      private double kurt;

      Stats(double mean, double std, double var, double skew, double kurt) {
        this.mean = mean;
        this.std = std;
        this.var = var;
        this.skew = skew;
        this.kurt = kurt;
      }
    }

    @Data
    private static class OrderHistory {
      private String orderId;
      private final double oppositePrice;
      private long createAt;
      private BitmexSide side;

      OrderHistory(String orderId, double oppositePrice, long createAt, BitmexSide side) {
        this.orderId = orderId;
        this.oppositePrice = oppositePrice;
        this.createAt = createAt;
        this.side = side;
      }
    }

    /*private Object predict(Map<String, Double> feature) {
      Map<FieldName, FieldValue> argsMap = new LinkedHashMap<>();
      List<InputField> activeFields = evaluator.getActiveFields();

      for (InputField activeField : activeFields) {
        final FieldName fieldName = activeField.getName();
        Object rawValue = feature.get(fieldName.getValue());
        FieldValue fieldValue = activeField.prepare(rawValue);
        argsMap.put(fieldName, fieldValue);
      }
      final Map<FieldName, ?> results = evaluator.evaluate(argsMap);
      ProbabilityDistribution pd = (ProbabilityDistribution) results.get(new FieldName(this.target));
      return pd.getResult();
    }*/
  }
}
