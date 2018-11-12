package io.magicalne.smym.strategy;

import com.google.common.annotations.VisibleForTesting;
import io.magicalne.smym.dto.bitmex.AlgoTrading;
import io.magicalne.smym.dto.bitmex.BitmexConfig;
import io.magicalne.smym.exchanges.bitmex.BitmexCancelOrderException;
import io.magicalne.smym.exchanges.bitmex.BitmexDeltaClient;
import io.magicalne.smym.exchanges.bitmex.BitmexExchange;
import io.magicalne.smym.exchanges.bitmex.BitmexQueryOrderException;
import io.swagger.client.ApiException;
import io.swagger.client.model.Order;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.*;
import org.jpmml.model.PMMLUtil;
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
        } catch (ApiException e) {
          log.error("Bad thing happened. FUCK! code: {}, body:{}, headers: {}, {}",
            e.getCode(), e.getResponseBody(), e.getResponseHeaders(), e);
        } catch (BitmexCancelOrderException e) {
          log.error("Cancel order failed.", e);
        }
      }
      Thread.sleep(5000);
    }

  }

  @Slf4j
  public static class OrderFlowPrediction {

    private final BitmexDeltaClient deltaClient;
    private final Evaluator evaluator;
    private final Queue<BitmexDeltaClient.OrderBookL2> queue;
    private final String symbol;
    private final int shortAmount;
    private final int longAmount;
    private final String pmmlPath;
    private final String target;
    private final BitmexExchange exchange;
    private double profit = 0;
    private String longOrderId;
    private double longPosition = -1;
    private boolean isLongFilled = false;
    private String shortOrderId;
    private double shortPosition = -1;
    private boolean isShortFilled = false;


    public OrderFlowPrediction(String deltaHost, int deltaPort, AlgoTrading config, BitmexExchange exchange)
      throws IOException, JAXBException, SAXException {
      this.deltaClient = new BitmexDeltaClient(deltaHost, deltaPort);
      String pmmlPath = config.getPmmlPath();
      this.evaluator = initPMML(pmmlPath);
      this.queue = new CircularFifoQueue<>(10);
      this.symbol = config.getSymbol();
      this.shortAmount = config.getShortAmount();
      this.longAmount = config.getLongAmount();
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
      log.info("Warming up is done.");
    }

    private void execute() throws IOException, ApiException, BitmexCancelOrderException {
      BitmexDeltaClient.OrderBookL2 orderBookL2 = deltaClient.getOrderBookL2(symbol);
      double bestAsk = orderBookL2.getBestAsk();
      double bestBid = orderBookL2.getBestBid();
      this.queue.add(orderBookL2);
      int prediction = (int) predict(extractFeature(queue));
      if (prediction == 1) {
        longStrategy(bestBid);
      } else if (prediction == -1) {
        shortStrategy(bestAsk);
      }

      stopLoss(bestBid, bestAsk);
    }

    private void shortStrategy(double bestAsk) throws IOException, ApiException, BitmexCancelOrderException {
      if (longPosition > 0) { //from long to short
        log.info("From long to short.");
        Order order;
        try {
          order = deltaClient.getOrderById(symbol, longOrderId);
        } catch (BitmexQueryOrderException e) {
          log.error("Query long order {} with exception: {}", longOrderId, e);
          return;
        }
        if (BitmexExchange.ORDER_STATUS_FILLED.equals(order.getOrdStatus()) ||
          BitmexExchange.ORDER_STATUS_CANCELED.equals(order.getOrdStatus())) {
          order = exchange.placeLimitShortOrder(symbol, bestAsk, order.getCumQty().intValue() + shortAmount);
          log.info("Just short: {}", order);
          shortOrderId = order.getOrderID();
          shortPosition = bestAsk;
          longPosition = -1;
          longOrderId = null;
        } else {
          log.info("Cancel long order: {}", longOrderId);
          exchange.cancel(longOrderId);
        }
        return;
      }
      if (shortPosition < 0) {
        Order order = exchange.placeLimitShortOrder(symbol, bestAsk, shortAmount);
        shortPosition = bestAsk;
        shortOrderId = order.getOrderID();
      } else {
        if (!isShortFilled) {
          Order order;
          try {
            order = this.deltaClient.getOrderById(symbol, shortOrderId);
          } catch (BitmexQueryOrderException e) {
            log.error("Query short order {} with exception: {}", shortOrderId, e);
            return;
          }
          if (BitmexExchange.ORDER_STATUS_FILLED.equals(order.getOrdStatus())) {
            isShortFilled = true;
            log.info("Short {} at {} FILLED.", order.getCumQty(), order.getPrice());
          } else if (shortPosition > bestAsk) { //need to amend price
            log.info("Amend short price from {} to {}.", shortPosition, bestAsk);
            this.exchange.amendOrderPrice(shortOrderId, bestAsk);
            shortPosition = bestAsk;
          }
        }
      }
    }

    private void longStrategy(double bestBid) throws ApiException, IOException, BitmexCancelOrderException {
      if (shortPosition > 0) { //from short to long
        log.info("From short to long.");
        Order order;
        try {
          order = deltaClient.getOrderById(symbol, shortOrderId);
        } catch (BitmexQueryOrderException e) {
          log.error("Query short order {} with exception: {}", shortOrderId, e);
          return;
        }
        if (BitmexExchange.ORDER_STATUS_FILLED.equals(order.getOrdStatus()) ||
          BitmexExchange.ORDER_STATUS_CANCELED.equals(order.getOrdStatus())) {
          order = exchange.placeLimitLongOrder(symbol, bestBid, order.getCumQty().intValue() + longAmount);
          log.info("Place short order: order");
          longOrderId = order.getOrderID();
          longPosition = bestBid;
          shortOrderId = null;
          shortPosition = -1;
        } else {
          exchange.cancel(shortOrderId);
          log.info("Cancel short order: {}", longOrderId);
        }
        return;
      }

      if (longPosition < 0) { //just long
        log.info("Just long for {} at best bid: {}, contacts: {}", symbol, bestBid, longAmount);
        Order order = exchange.placeLimitLongOrder(symbol, bestBid, longAmount);
        log.info("long order: {}", order);
        longPosition = bestBid;
        longOrderId = order.getOrderID();
      } else { //check filled or not
        if (!isLongFilled) {
          Order order;
          try {
            order = this.deltaClient.getOrderById(symbol, longOrderId);
          } catch (BitmexQueryOrderException e) {
            log.error("Query long order {} with exception: {}", longOrderId, e);
            return;
          }
          if (BitmexExchange.ORDER_STATUS_FILLED.equals(order.getOrdStatus())) {
            isLongFilled = true;
            log.info("Long {} at {} FILLED.", order.getCumQty(), order.getPrice());
          } else if (longPosition < bestBid) { //need to amend price
            log.info("Amend long price from {} to {}.", longPosition, bestBid);
            this.exchange.amendOrderPrice(longOrderId, bestBid);
            longPosition = bestBid;
          }
        }
      }
    }

    private void stopLoss(double bestBid, double bestAsk) throws ApiException {
      if (isLongFilled) {
        if (longPosition > bestBid) {
          exchange.placeMarketShortOrder(symbol, longAmount);
          log.info("LONG STOP LOSS: from {} to {}", longPosition, bestBid);
          isLongFilled = false;
          longPosition = -1;
          longOrderId = null;
        }
      } else if (isShortFilled) {
        if (shortPosition < bestAsk) {
          exchange.placeMarketLongOrder(symbol, shortAmount);
          log.info("SHORT STOP LOSS: from {} to {}", shortPosition, bestAsk);
          isShortFilled = false;
          shortPosition = -1;
          shortOrderId = null;
        }
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
