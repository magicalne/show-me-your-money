package io.magicalne.smym.strategy;

import com.google.common.annotations.VisibleForTesting;
import io.magicalne.smym.dto.bitmex.AlgoTrading;
import io.magicalne.smym.dto.bitmex.BitmexConfig;
import io.magicalne.smym.exchanges.bitmex.BitmexDeltaClient;
import io.magicalne.smym.exchanges.bitmex.BitmexExchange;
import io.magicalne.smym.exchanges.bitmex.BitmexQueryOrderException;
import io.swagger.client.model.Order;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.*;
import org.jpmml.model.PMMLUtil;
import org.knowm.xchange.bitmex.dto.marketdata.BitmexPrivateOrder;
import org.knowm.xchange.bitmex.dto.trade.BitmexSide;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.exceptions.ExchangeException;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.InputStream;
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

  public void execute()
    throws IOException, InterruptedException, JAXBException, SAXException {
    List<AlgoTrading> algoTradings = config.getAlgoTradings();
    List<OrderFlowPrediction> list = new LinkedList<>();
    for (AlgoTrading a : algoTradings) {
      OrderFlowPrediction afp = new OrderFlowPrediction(config.getDeltaHost(), config.getDeltaPort(), a, exchange);
      afp.setup();
      list.add(afp);
    }

    for (;;) {
      for (OrderFlowPrediction ofp : list) {
        try {
          ofp.execute();
        } catch (Exception e) {
          log.error("Trading with exception: ", e);
        }
      }
    }

  }

  @Slf4j
  public static class OrderFlowPrediction {

    private final BitmexDeltaClient deltaClient;
    private final Evaluator evaluator;
    private final Queue<BitmexDeltaClient.OrderBookL2> queue;
    private final String symbol;
    private final int contracts;
    private final String pmmlPath;
    private final String target;
    private final BitmexExchange exchange;
    private final CurrencyPair currencyPair;
    private String longOrderId;
    private double longPosition = -1;
    private String shortOrderId;
    private double shortPosition = -1;
    private long start;
    static final double OFFSET = 0.5;
    private int prediction;

    OrderFlowPrediction(String deltaHost, int deltaPort, AlgoTrading config, BitmexExchange exchange)
      throws IOException, JAXBException, SAXException {
      this.deltaClient = new BitmexDeltaClient(deltaHost, deltaPort);
      String pmmlPath = config.getPmmlPath();
      this.evaluator = initPMML(pmmlPath);
      this.queue = new CircularFifoQueue<>(10);
      currencyPair = new CurrencyPair(config.getSymbol());
      this.symbol = this.currencyPair.base.getCurrencyCode() + this.currencyPair.counter.getCurrencyCode();
      this.contracts = config.getContracts();
      this.pmmlPath = config.getPmmlPath();
      this.target = config.getTarget();
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

    private void setup() throws InterruptedException, IOException {
      log.info("Warming up...");
      for (int i = 0; i < 10; i ++) {
        this.queue.add(deltaClient.getOrderBookL2(symbol));
        Thread.sleep(5000);
      }
      this.start = System.currentTimeMillis();
      log.info("Warming up is done.");
    }

    private void execute() throws IOException {
      long now = System.currentTimeMillis();
      BitmexDeltaClient.OrderBookL2 orderBookL2 = deltaClient.getOrderBookL2(symbol);
      double bestAsk = orderBookL2.getBestAsk();
      double bestBid = orderBookL2.getBestBid();
      if (now - start < 5000) {
        //amend order
        if (longOrderId != null && shortOrderId != null) {
          if (prediction == 0) {
            tryAmendEven(bestBid, bestAsk);
          } else {
            tryAmend(bestBid, bestAsk);
          }
        }
      } else {
        this.start = System.currentTimeMillis();
        this.queue.add(orderBookL2);
        if (longOrderId == null && shortOrderId == null) {
          prediction = (int) predict(extractFeature(queue));
          List<BitmexPrivateOrder> orderPair;
          if (prediction == 1) {
            orderPair = exchange.placePairOrders(symbol, bestBid, bestAsk + OFFSET, contracts);
          } else if (prediction == -1) {
            orderPair = exchange.placePairOrders(symbol, bestBid - OFFSET, bestAsk, contracts);
          } else {
            orderPair = exchange.placePairOrders(symbol, bestBid, bestAsk, contracts);
          }
          BitmexPrivateOrder order1 = orderPair.get(0);
          BitmexPrivateOrder order2 = orderPair.get(1);
          if (BitmexSide.BUY == order1.getSide()) {
            longOrderId = order1.getId();
            longPosition = order1.getPrice().doubleValue();
            shortOrderId = order2.getId();
            shortPosition = order2.getPrice().doubleValue();
          } else {
            longOrderId = order2.getId();
            longPosition = order2.getPrice().doubleValue();
            shortOrderId = order1.getId();
            shortPosition = order2.getPrice().doubleValue();
          }
          log.info("Prediction: {}.", prediction);
          log.info("Place long order at {}.", longPosition);
          log.info("place short order at {}.", shortPosition);
        }
      }
    }

    private void tryAmendEven(double bestBid, double bestAsk) {
      try {
        Order longOrder = deltaClient.getOrderById(symbol, longOrderId);
        Order shortOrder = deltaClient.getOrderById(symbol, shortOrderId);
        boolean isLongFilled = BitmexExchange.ORDER_STATUS_FILLED.equals(longOrder.getOrdStatus());
        boolean isShortFilled = BitmexExchange.ORDER_STATUS_FILLED.equals(shortOrder.getOrdStatus());
        if (isLongFilled) {
          if (!isShortFilled) {
            if (bestBid < longPosition) { //market goes to low, fast short
              int compare = Double.compare(longPosition, shortPosition);
              if (compare < 0) {
                exchange.amendOrderPrice(shortOrderId, contracts, longPosition);
                shortPosition = longPosition;
                log.info("Long order is filled at {}, but short order is not, amend short order to {}",
                  longPosition, shortPosition);
              }
            }
          } else {
            log.info("Both bid and ask were filled. bid: {}, ask: {}", longPosition, shortPosition);
            longPosition = -1;
            longOrderId = null;
            shortPosition = -1;
            shortOrderId = null;
          }
        } else {
          if (isShortFilled) {
            if (bestAsk > shortPosition) { //market goes to high, fast long
              int compare = Double.compare(longPosition, shortPosition);
              if (compare < 0) {
                exchange.amendOrderPrice(longOrderId, contracts, shortPosition);
                longPosition = shortPosition;
                log.info("Short order is filled at {}, but long order is not, amend long order to {}",
                  shortPosition, longPosition);
              }
            }
          }
        }
      } catch (BitmexQueryOrderException | IOException | ExchangeException e) {
        log.error("Amend long order with exception.", e);
      }
    }

    private void tryAmend(double bestBid, double bestAsk) {
      try {
        Order longOrder = deltaClient.getOrderById(symbol, longOrderId);
        Order shortOrder = deltaClient.getOrderById(symbol, shortOrderId);
        boolean isLongFilled = BitmexExchange.ORDER_STATUS_FILLED.equals(longOrder.getOrdStatus());
        boolean isShortFilled = BitmexExchange.ORDER_STATUS_FILLED.equals(shortOrder.getOrdStatus());
        if (isLongFilled) {
          if (isShortFilled) {
            log.info("Both bid and ask were filled. bid: {}, ask: {}", longPosition, shortPosition);
            longPosition = -1;
            longOrderId = null;
            shortPosition = -1;
            shortOrderId = null;
          } else {
            int compare = Double.compare(longPosition, bestBid);
            if (compare == -1 || compare == 0) { //market still goes to high
              if (Double.compare(shortPosition, bestAsk) > 0) {
                exchange.amendOrderPrice(shortOrderId, contracts, bestAsk);
                shortPosition = bestAsk;
                log.info("Long order is filled at {}, but short order is not, amend short order to {}",
                  longPosition, shortPosition);
              }
            } else { //market goes to low
              if (Double.compare(shortPosition, longPosition) > 0) {
                exchange.amendOrderPrice(shortOrderId, contracts, longPosition);
                shortPosition = longPosition;
                log.info("Long order is filled at {}, but short order is not, amend short order to {}",
                  longPosition, shortPosition);
              }
            }
          }
        } else {
          if (isShortFilled) {
            int compare = Double.compare(shortPosition, bestAsk);
            if (compare == 1 || compare == 0) { //market goes to low
              if (Double.compare(longPosition, bestBid) < 0) {
                exchange.amendOrderPrice(longOrderId, contracts, bestBid);
                longPosition = bestBid;
                log.info("Short order is filled at {}, but long order is not, amend short oder to: {}",
                  shortPosition, longPosition);
              }
            } else { //market still goes to high
              if (Double.compare(shortPosition, longPosition) > 0) {
                exchange.amendOrderPrice(longOrderId, contracts, shortPosition);
                longPosition = shortPosition;
                log.info("Short order is filled at {}, but long order is not, amend short oder to: {}",
                  shortPosition, longPosition);
              }
            }
          } else { // both sides are not filled
            if (prediction == 1) {
              if (Double.compare(longPosition, bestBid) == -1) {
                longPosition = bestBid;
                shortPosition = bestAsk + OFFSET;
                exchange.amendPairOrder(longOrderId, longPosition, shortOrderId, shortPosition, contracts);
                log.info("Prediction: {}. No filled. Amend: long {}, short {}",
                  prediction, longPosition, shortPosition);
              }
            } else if (prediction == -1) {
              if (Double.compare(shortPosition, bestAsk) == 1) {
                longPosition = bestBid - OFFSET;
                shortPosition = bestAsk;
                exchange.amendPairOrder(longOrderId, longPosition, shortOrderId, shortPosition, contracts);
                log.info("Prediction: {}. No filled. Amend: long {}, short {}",
                  prediction, longPosition, shortPosition);
              }
            }
          }
        }
      } catch (IOException | BitmexQueryOrderException | ExchangeException e) {
        log.error("Amend long order with exception.", e);
      }
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
        for (int i = 0; i < 10; i ++) {
          BitmexDeltaClient.OrderBookEntry ask = asks.get(i);
          BitmexDeltaClient.OrderBookEntry bid = bids.get(i);
          featureMap.put("ask_p_"+i, ask.getPrice());
          featureMap.put("ask_vol_"+i, (double) ask.getSize());
          featureMap.put("bid_p_"+i, bid.getPrice());
          featureMap.put("bid_vol_"+i, (double) bid.getSize());
          double spreed = ask.getPrice() - bid.getPrice();
          spreedSum += spreed;
          featureMap.put("spreed_"+i, spreed);
          featureMap.put("mid_p_"+i, (ask.getPrice() + bid.getPrice()) / 2);
          double spreedVol = ask.getSize() - bid.getSize();
          featureMap.put("spreed_vol_"+i, spreedVol);
          spreedVolSum += spreedVol;
          featureMap.put("vol_rate_"+i, ask.getSize()*1.0d / bid.getSize());
          featureMap.put("sask_vol_rate_"+i, spreedVol / ask.getSize());
          featureMap.put("sbid_vol_rate_"+i, spreedVol / bid.getSize());
          double volSum = ask.getSize() + bid.getSize();
          featureMap.put("ask_vol_rate_"+i, ask.getSize() / volSum);
          featureMap.put("bid_vol_rate_"+i, bid.getSize() / volSum);
          askPriceSum += ask.getPrice();
          bidPriceSum += bid.getPrice();
          askVolSum += ask.getSize();
          bidVolSum += bid.getSize();
        }
        for (int i = 0; i < 9; i ++) {
          int k = i + 1;
          featureMap.put("ask_p_diff_"+k+"_0", featureMap.get("ask_p_"+k) - featureMap.get("ask_p_0"));
          featureMap.put("bid_p_diff_"+k+"_0", featureMap.get("bid_p_"+k) - featureMap.get("bid_p_0"));
          featureMap.put("ask_p_diff_"+k+"_"+i, featureMap.get("ask_p_"+k) - featureMap.get("ask_p_"+i));
          featureMap.put("bid_p_diff_"+k+"_"+i, featureMap.get("bid_p_"+k) - featureMap.get("bid_p_"+i));
        }
        featureMap.put("ask_p_mean", askPriceSum/10);
        featureMap.put("bid_p_mean", bidPriceSum/10);
        featureMap.put("ask_vol_mean", askVolSum/10);
        featureMap.put("bid_vol_mean", bidVolSum/10);
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
      for (int i = 9; i >= 2; i --) {
        askPrices.remove(0);
        extractTSFeature(featureMap, askPrices, "ask_p_roll_"+i);
        bidPrices.remove(0);
        extractTSFeature(featureMap, bidPrices, "bid_p_roll_"+i);
        askVols.remove(0);
        extractTSFeature(featureMap, askVols, "ask_v_roll_"+i);
        bidVols.remove(0);
        extractTSFeature(featureMap, bidVols, "bid_v_roll_"+i);
      }
      return featureMap;
    }

    private static void extractTSFeature(Map<String, Double> featureMap, List<Double> askPrices, String prefix) {
      Stats askPriceStats = timeSeriesFeature(askPrices);
      featureMap.put(prefix+"_mean", askPriceStats.getMean());
      featureMap.put(prefix+"_std", askPriceStats.getStd());
      featureMap.put(prefix+"_var", askPriceStats.getVar());
      featureMap.put(prefix+"_skew", askPriceStats.getSkew());
      featureMap.put(prefix+"_kurt", askPriceStats.getKurt());
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

    private Object predict(Map<String, Double> feature) {
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
    }
  }
}
