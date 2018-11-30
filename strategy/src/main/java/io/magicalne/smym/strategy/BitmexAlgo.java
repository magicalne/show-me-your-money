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

  public void execute(){
    List<AlgoTrading> algoTradings = config.getAlgoTradings();
    List<MarketMaker> list = new LinkedList<>();
    for (AlgoTrading a : algoTradings) {
      MarketMaker afp = new MarketMaker(config.getDeltaHost(), config.getDeltaPort(), a, exchange);
      afp.setup();
      list.add(afp);
    }

    for (;;) {
      for (MarketMaker ofp : list) {
        try {
          ofp.execute();
        } catch (ExchangeException e) {
          log.error("Bitmex exehange exception: ", e);
        } catch (Exception e) {
          log.error("Trading with exception: ", e);
        }
      }
    }

  }

  @Slf4j
  public static class MarketMaker {

    private static final int TIMEOUT = 60*60*1000; //1 hour
    private static final double STOP_LOSS = 0.01;
    private static final String STOP_LOSS_FROM = "STOP LOSS from ";
    private static final int SIZE_THRESHOLD = 250000;
    private final BitmexDeltaClient deltaClient;
    private final String symbol;
    private final int contracts;
    private final double leverage;
    private final BitmexExchange exchange;
    private final CurrencyPair currencyPair;
    private final double imbalance;
    private final int count;
    private final List<OrderHistory> waitings = new ArrayList<>();
    private String longOrderId;
    private String shortOrderId;
    private double longPrice;
    private double shortPrice;
    private long createAt;
    private double lastMidPrice;
    private static final double TICK = 0.5;
    private static final double IMBALANCE = 0.3;
    private boolean longFilled = false;
    private boolean shortFilled = false;

    MarketMaker(String deltaHost, int deltaPort, AlgoTrading config, BitmexExchange exchange) {
      this.deltaClient = new BitmexDeltaClient(deltaHost, deltaPort);
      currencyPair = new CurrencyPair(config.getSymbol());
      this.symbol = this.currencyPair.base.getCurrencyCode() + this.currencyPair.counter.getCurrencyCode();
      this.contracts = config.getContracts();
      this.leverage = config.getLeverage();
      this.imbalance = config.getImbalance();
      this.count = config.getCount();
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
      if (this.leverage > 0) {
        this.exchange.setLeverage(symbol, leverage);
      }
    }

    private void execute() throws IOException {
      try {
        placeOrders();
      } catch (BitmexQueryOrderException ignore) {

      }
    }

    private void placeOrders() throws IOException, BitmexQueryOrderException {
      BitmexDeltaClient.Trades trade = deltaClient.getTrade(symbol);
      BitmexDeltaClient.Stats stats = trade.recentStats(5000);
      double mi = stats == null ? 3 : stats.getVolImbalance();
      BitmexDeltaClient.OrderBookL2 ob = deltaClient.getOrderBookL2(symbol);
      BitmexDeltaClient.OrderBookEntry bestBid = ob.getBestBid();
      BitmexDeltaClient.OrderBookEntry bestAsk = ob.getBestAsk();
      int t = SIZE_THRESHOLD;
      double obi = ob.imbalance();
      if (longOrderId == null && shortOrderId == null) {
        if (-IMBALANCE < obi && obi < IMBALANCE
          && -IMBALANCE < mi && mi < IMBALANCE
          && bestBid.getSize() >= t && bestAsk.getSize() >= t) {
          List<BitmexPrivateOrder> orderPair =
            this.exchange.placePairOrders(symbol, bestBid.getPrice(), bestAsk.getPrice(), contracts);
          this.longOrderId = orderPair.get(0).getId();
          this.longPrice = bestBid.getPrice();
          this.shortOrderId = orderPair.get(1).getId();
          this.shortPrice = bestAsk.getPrice();
        } else if (mi < 2 && mi > IMBALANCE && bestAsk.getSize() > SIZE_THRESHOLD) {
          //market buy is taking the lead, place short limit order
          BitmexPrivateOrder order = this.exchange.placeLimitOrder(symbol, bestAsk.getPrice(), contracts, BitmexSide.SELL);
          this.shortPrice = bestAsk.getPrice();
          this.shortOrderId = order.getId();
        } else if (mi < 2 && -IMBALANCE < mi && bestBid.getSize() > SIZE_THRESHOLD) {
          BitmexPrivateOrder order = this.exchange.placeLimitOrder(symbol, bestBid.getPrice(), contracts, BitmexSide.BUY);
          this.longPrice = bestBid.getPrice();
          this.longOrderId = order.getId();
        }
      } else {
        if (longOrderId != null) {
          BitmexPrivateOrder longOrder = deltaClient.getOrderById(symbol, longOrderId);
          if (longOrder.getOrderStatus() == BitmexPrivateOrder.OrderStatus.Filled) {
            log.info("Long order filled at {}.", longPrice);
            this.longOrderId = null;
            longFilled = true;
            if (shortFilled) {
              longFilled = false;
              shortFilled = false;
              return;
            }
            if (shortOrderId == null) {
              BitmexPrivateOrder order = exchange.placeLimitOrder(symbol, bestAsk.getPrice(), contracts, BitmexSide.SELL);
              this.shortOrderId = order.getId();
              this.shortPrice = bestAsk.getPrice();
              log.info("Place short order at {}.", shortPrice);
            }
          } else {
            if (mi < 2 && -IMBALANCE < mi && shortFilled) { // market sell taking the lead now, not good for long limit order.
              boolean cancel = exchange.cancel(longOrderId);
              if (cancel) {
                log.info("Cancel long order at {} due to too many market sell volumes.", longPrice);
                this.longOrderId = null;
              } else {
                log.warn("Cancel long order failed.");
              }
            }
          }
        } else {
          if (mi > IMBALANCE && bestBid.getSize() >= SIZE_THRESHOLD) {
            BitmexPrivateOrder order = exchange.placeLimitOrder(symbol, bestBid.getPrice(), contracts, BitmexSide.BUY);
            this.longOrderId = order.getId();
            this.longPrice = bestBid.getPrice();
            log.info("Place limit long order at {}.", this.longPrice);
          }
        }

        if (shortOrderId != null) {
          BitmexPrivateOrder shortOrder = deltaClient.getOrderById(symbol, shortOrderId);
          if (shortOrder.getOrderStatus() == BitmexPrivateOrder.OrderStatus.Filled) {
            log.info("Short order filled at {}.", shortPrice);
            this.shortOrderId = null;
            shortFilled = true;
            if (longFilled) {
              shortFilled = false;
              longFilled = false;
            }
          } else {
            if (mi < 2 && mi > IMBALANCE) { // market buy taking the lead now, not good for short limit order.
              boolean cancel = exchange.cancel(shortOrderId);
              if (cancel) {
                log.info("Cancel short order at {} due to too many market buy volumes.", shortPrice);
                this.shortOrderId = null;
              } else {
                log.warn("Cancel short order failed.");
              }
            }
          }
        } else {
          if (-IMBALANCE < mi && bestAsk.getSize() >= SIZE_THRESHOLD) {
            BitmexPrivateOrder order = exchange.placeLimitOrder(symbol, bestAsk.getPrice(), contracts, BitmexSide.SELL);
            this.shortOrderId = order.getId();
            this.shortPrice = bestAsk.getPrice();
            log.info("Place limit short order at {}.", this.shortPrice);
          }
        }
      }
    }

    private void stopLoss() {

    }

    private BitmexPrivateOrder tryAmendLongOrder(String orderId, double price, boolean longCanceled) {
      return longCanceled ? exchange.placeLimitOrder(symbol, price, contracts, BitmexSide.BUY) :
        exchange.amendOrderPrice(orderId, contracts, price);
    }

    private BitmexPrivateOrder tryAmendShortOrder(String orderId, double price, boolean shortCanceled) {
      return shortCanceled ? exchange.placeLimitOrder(symbol, price, contracts, BitmexSide.SELL) :
        exchange.amendOrderPrice(orderId, contracts, price);
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
