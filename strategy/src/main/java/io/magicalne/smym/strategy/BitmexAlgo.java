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

    private BitmexPrivateOrder bidOrder = null;
    private BitmexPrivateOrder askOrder = null;
    private Position position = null;
//    private boolean bidFilled = false;
//    private boolean askFilled = false;
    private double profit = 0;
    private int bidContract;
    private int askContract;
    private long bidPrice;
    private long askPrice;
    private double profitableAsk = -1;
    private double profitableBid = Integer.MAX_VALUE;

    MarketMaker(String deltaHost, int deltaPort, AlgoTrading config, BitmexExchange exchange) {
      this.deltaClient = new BitmexDeltaClient(deltaHost, deltaPort);
      this.symbol = config.getSymbol();
      this.contract = config.getContract();
      this.leverage = config.getLeverage();
      this.spread = config.getSpread();
      this.exchange = exchange;
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
      double imbalance = ob.imbalance();
      double mid = (bestBid + bestAsk) / 2;
      if (bidOrder == null && askOrder == null && -IMBALANCE < imbalance && imbalance < IMBALANCE) {
        bidPrice = Math.round(bestBid - TICK);
        askPrice = Math.round(bestAsk + TICK);
        List<BitmexPrivateOrder> orders = exchange.placePairOrders(symbol, bidPrice, askPrice, contract);
        bidContract = contract;
        askContract = contract;
        bidOrder = orders.get(0);
        askOrder = orders.get(1);
        log.info("Place bid at {}, ask: {}", bidPrice, askPrice);
      } else {
        if (bidOrder != null) {
          BitmexPrivateOrder bid = deltaClient.getOrderById(symbol, bidOrder.getId());
          if (bid.getOrderStatus() == BitmexPrivateOrder.OrderStatus.Filled) {
            log.info("Bid filled at {}", bid.getPrice());
            if (position == null) {
              openLongPosition(mid, bid);
            } else {
              if (position.getPrice() > 0) {
                supplyLongPosition(mid);
              } else {
                if (position.getContract() == bidContract) {
                  bidFilledProfit();
                  reset();
                } else {
                  closeShortPositionAndOpenLongPosition(bestBid, bestAsk);
                }
              }
            }
            log.info("Position: {}", position);
            log.info("place bid: {} * {}, ask: {} * {}", bidPrice, bidContract, askPrice, askContract);
          } else if (bid.getOrderStatus() == BitmexPrivateOrder.OrderStatus.Canceled) {
            this.bidOrder = null;
          }
        }
        if (askOrder != null) {
          BitmexPrivateOrder ask = deltaClient.getOrderById(symbol, askOrder.getId());
          if (ask.getOrderStatus() == BitmexPrivateOrder.OrderStatus.Filled) {
            log.info("Ask filled at {}", ask.getPrice());
            if (position == null) {
              openShortPosition(mid, ask);
            } else {
              if (position.getPrice() < 0) {
                supplyShortPosition(mid);
              } else {
                if (position.getContract() == askContract) {
                  askFilledProfit();
                  reset();
                } else {
                  closeLongPositionAndOpenShortPosition(bestBid, bestAsk);
                }
              }
              log.info("Position: {}", position);
              log.info("bid order: {} * {}, ask order: {} * {}", bidPrice, bidContract, askPrice, askContract);
            }
          } else if (ask.getOrderStatus() == BitmexPrivateOrder.OrderStatus.Canceled) {
            this.askOrder = null;
          }
        }
      }

      if (position != null) {
        if (position.getPrice() > 0) {
          if (Double.compare(bestAsk, position.getPrice()) > 0 && imbalance > IMBALANCE) {
            if (Double.compare(bestAsk, profitableAsk) > 0) {
              profitableAsk = bestAsk;
            } else if (Double.compare(askPrice, bestAsk) != 0) {
              askOrder = exchange.amendOrderPrice(askOrder.getId(), contract, bestAsk);
              log.info("Amend ask order to {}.", bestAsk);
            }
          }
        } else if (position.getPrice() < 0) {
          if (Double.compare(bestBid, -position.getPrice()) < 0 && imbalance < -IMBALANCE) {
            if (Double.compare(bestBid, profitableBid) < 0) {
              profitableBid = bestBid;
            } else if (Double.compare(bidPrice, bestBid) != 0) {
              bidOrder = exchange.amendOrderPrice(bidOrder.getId(), contract, bestBid);
              log.info("Amend bid order to {}.", bestBid);
            }
          }
        }
      }
    }

    private void openLongPosition(double mid, BitmexPrivateOrder bid) {
      double newAsk = mid * (1 + spread);
      long askPrice = Math.round((askContract * this.askPrice + newAsk * contract) / (askContract + contract));
      long bidPrice = Math.round(mid * (1 - spread));
      askOrder = exchange.amendOrderPrice(askOrder.getId(), askContract+contract, askPrice);
      bidOrder = exchange.placeLimitOrder(symbol, bidPrice, bidContract, BitmexSide.BUY);
      this.bidPrice = bidPrice;
      this.askPrice = askPrice;
      askContract += contract;
      position = new Position(bid.getPrice().doubleValue(), bidContract);
    }

    private void supplyLongPosition(double mid) {
      int c = bidContract + position.getContract();
      double p = c / (bidContract * 1.0d / bidPrice + position.getContract() / position.getPrice());
      double skew = getSkew(c, askContract, spread);
      double newMid = mid * (1 + skew);
      double newAsk = newMid * (1 + spread);
      long askPrice = Math.round((askContract * this.askPrice + newAsk * contract) / (askContract + contract));
      long bidPrice = Math.round(newMid * (1 - spread));
      askOrder = exchange.amendOrderPrice(askOrder.getId(), askContract+contract, askPrice);
      bidOrder = exchange.placeLimitOrder(symbol, bidPrice, bidContract, BitmexSide.BUY);
      this.bidPrice = bidPrice;
      this.askPrice = askPrice;
      askContract += contract;
      position.update(p, c);
    }

    private void closeShortPositionAndOpenLongPosition(double bestBid, double bestAsk) {
      long bidPrice = Math.round(bestBid * (1 - spread));
      double newAsk = bestAsk * (1 + spread);
      long askPrice = Math.round((askContract * this.askPrice + contract * newAsk) / (askContract + contract));
      askOrder = exchange.amendOrderPrice(askOrder.getId(), askContract + contract, askPrice);
      bidOrder = exchange.placeLimitOrder(symbol, bidPrice, contract, BitmexSide.BUY);
      position.update(this.bidPrice, bidContract - position.getContract());
      this.bidPrice = bidPrice;
      this.askPrice = askPrice;
      bidContract = contract;
      askContract += contract;
    }

    private void openShortPosition(double mid, BitmexPrivateOrder ask) {
      long bidPrice = Math.round((bidContract * this.bidPrice + mid * contract) / (bidContract + contract));
      long askPrice = Math.round(mid * (1 + spread));
      bidOrder = exchange.amendOrderPrice(bidOrder.getId(), bidContract + contract, bidPrice);
      askOrder = exchange.placeLimitOrder(symbol, askPrice, askContract, BitmexSide.SELL);
      this.bidPrice = bidPrice;
      this.askPrice = askPrice;
      bidContract += contract;
      position = new Position(-ask.getPrice().doubleValue(), askContract);

    }

    private void supplyShortPosition(double mid) {
      int c = askContract + position.getContract();
      double p = c / (askContract * 1.0d / askPrice - position.getContract() / position.getPrice());
      double skew = getSkew(-c, bidContract, spread);
      double newMid = mid * (1 + skew);
      double newBid = newMid * (1 - spread);
      long bidPrice = Math.round((bidContract * this.bidPrice + newBid * contract) / (bidContract + contract));
      long askPrice = Math.round(newMid * (1 + spread));
      bidOrder = exchange.amendOrderPrice(bidOrder.getId(), bidContract + contract, bidPrice);
      askOrder = exchange.placeLimitOrder(symbol, askPrice, askContract, BitmexSide.SELL);
      this.bidPrice = bidPrice;
      this.askPrice = askPrice;
      bidContract += contract;
      position.update(-p, c);
    }

    private void closeLongPositionAndOpenShortPosition(double bestBid, double bestAsk) {
      long askPrice = Math.round(bestAsk * (1 + spread));
      double newBid = bestBid * (1 - spread);
      long bidPrice = Math.round((bidContract * this.bidPrice + newBid * contract) / (bidContract + contract));
      bidOrder = exchange.amendOrderPrice(bidOrder.getId(), bidContract + contract, bidPrice);
      askOrder = exchange.placeLimitOrder(symbol, askPrice, contract, BitmexSide.SELL);
      position.update(-this.askPrice, askContract - position.getContract());
      this.bidPrice = bidPrice;
      this.askPrice = askPrice;
      bidContract += contract;
      askContract = contract;
    }

    private double getSkew(int change, int total, double spread) {
      return (1.0d * change / total) * spread * -1;
    }

    private void reset() {
      bidOrder = null;
      askOrder = null;
      position = null;
      bidContract = contract;
      askContract = contract;
      profitableAsk = -1;
      profitableBid = Integer.MAX_VALUE;
      log.info("Reset.");
    }


    private void bidFilledProfit() {
      double p = position.getContract() * (1.0/bidPrice + 1/ position.getPrice())
        + REBATE * position.getContract() * (1.0 / bidPrice - 1 / position.getPrice());
      profit += p;
      log.info("Profit of this round: {}, total profit: {}", p, profit);
    }

    private void askFilledProfit() {
      double p = position.getContract() * (1/ position.getPrice() - 1.0 / askPrice)
        + REBATE * position.getContract() * (1 / position.getPrice()  + 1.0 / askPrice);
      profit += p;
      log.info("Profit of this round: {}, total profit: {}", p, profit);
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

    private void stopLoss() {

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
