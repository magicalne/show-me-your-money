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
      if (longOrderId != null && shortOrderId != null) {
        amendPrice();
      } else {
        if (waitings.size() < count) {
          placeBidAskOrders();
        }
      }
      if (waitings.size() > 0) {
        stopLoss();
      }
    }

    private void stopLoss() {
      try {
        BitmexDeltaClient.OrderBookL2 ob = deltaClient.getOrderBookL2(symbol);
        double bestAsk = ob.getBestAsk();
        double bestBid = ob.getBestBid();

        Iterator<OrderHistory> iterator = waitings.iterator();
        while (iterator.hasNext()) {
          OrderHistory o = iterator.next();
          String orderId = o.getOrderId();
          Order order = deltaClient.getOrderById(symbol, orderId);
          boolean filled = BitmexExchange.ORDER_STATUS_FILLED.equals(order.getOrdStatus());
          boolean canceled = BitmexExchange.ORDER_STATUS_CANCELED.equals(order.getOrdStatus());
          if (filled) {
            if (o.getSide() == BitmexSide.BUY) {
              log.info("Both filled! Long: {}, short: {}.", order.getPrice(), o.getOppositePrice());
            } else {
              log.info("Both filled! Long: {}, short: {}.", o.getOppositePrice(), order.getPrice());
            }
            iterator.remove();
          } else if (canceled) {
            log.info("Place order at stop loss phase for order: {}", o);
            int m = 10;
            int oppositeSnapshot = (int) (o.getOppositePrice() * m);
            int bestBidSnapshot = (int) (bestBid * m);
            int bestAskSnapshot = (int) (bestAsk * m);
            if (BitmexSide.BUY == o.getSide()) {
              if (oppositeSnapshot > bestBidSnapshot) {
                BitmexPrivateOrder bitmexPrivateOrder =
                  this.exchange.placeLimitOrder(symbol, bestBid, contracts, BitmexSide.BUY);
                o.setOrderId(bitmexPrivateOrder.getId());
              } else {
                double newLongPrice = o.getOppositePrice() - TICK;
                BitmexPrivateOrder bitmexPrivateOrder =
                  this.exchange.placeLimitOrder(symbol, newLongPrice, contracts, BitmexSide.BUY);
                o.setOrderId(bitmexPrivateOrder.getId());
              }
            } else {
              if (oppositeSnapshot < bestAskSnapshot) {
                BitmexPrivateOrder bitmexPrivateOrder =
                  this.exchange.placeLimitOrder(symbol, bestAsk, contracts, BitmexSide.SELL);
                o.setOrderId(bitmexPrivateOrder.getId());
              } else {
                double newShortPrice = o.getOppositePrice() + TICK;
                BitmexPrivateOrder bitmexPrivateOrder =
                  this.exchange.placeLimitOrder(symbol, newShortPrice, contracts, BitmexSide.SELL);
                o.setOrderId(bitmexPrivateOrder.getId());
              }
            }
          } else {
            long createAt = o.getCreateAt();
            long timeout = System.currentTimeMillis() - createAt;
            double price = o.getOppositePrice();
            if (BitmexSide.BUY == o.getSide()) {
              double loss = (bestAsk - price) / price;
              if (timeout >= TIMEOUT || loss >= STOP_LOSS) {
                boolean cancel = this.exchange.cancel(orderId);
                log.info("Cancel {} : {}", orderId, cancel);
                iterator.remove();
              }
            } else {
              double loss = (price - bestBid) / price;
              if (timeout >= TIMEOUT || loss >= STOP_LOSS) {
                boolean cancel = this.exchange.cancel(orderId);
                log.info("Cancel {} : {}", orderId, cancel);
                iterator.remove();
              }
            }
          }
        }
      } catch (BitmexQueryOrderException | IOException ignored) {
      }

    }

    private void placeBidAskOrders() throws IOException {
      BitmexDeltaClient.OrderBookL2 ob = deltaClient.getOrderBookL2(symbol);
      double imb = ob.imbalance();
      double bestAsk = ob.getBestAsk();
      double bestBid = ob.getBestBid();
      this.lastMidPrice = (bestAsk + bestBid) / 2;
      List<BitmexPrivateOrder> orderPair = null;
      if (-this.imbalance <= imb && imb <= this.imbalance) {
        orderPair = this.exchange.placePairOrders(symbol, bestBid - TICK, bestAsk + TICK, this.contracts);
      } else if (imb < -this.imbalance) {
        orderPair = this.exchange.placePairOrders(symbol, ob.findFairBid(), bestAsk + TICK, this.contracts);
      } else if (imb > this.imbalance) {
        orderPair = this.exchange.placePairOrders(symbol, bestBid - TICK, ob.findFairAsk(), this.contracts);
      }
      if (orderPair != null) {
        BitmexPrivateOrder bid = orderPair.get(0);
        this.longPrice = bid.getPrice().doubleValue();
        this.longOrderId = bid.getId();

        BitmexPrivateOrder ask = orderPair.get(1);
        this.shortPrice = ask.getPrice().doubleValue();
        this.shortOrderId = ask.getId();

        log.info("Place long order at {}.", longPrice);
        log.info("Place short order at {}.", shortPrice);
        this.createAt = System.currentTimeMillis();
      }
    }

    private void amendPrice() throws IOException {
      BitmexDeltaClient.OrderBookL2 ob = deltaClient.getOrderBookL2(symbol);
      double bestAsk = ob.getBestAsk();
      double bestBid = ob.getBestBid();
      Order longOrder, shortOrder;
      try {
        longOrder = deltaClient.getOrderById(symbol, longOrderId);
        shortOrder = deltaClient.getOrderById(symbol, shortOrderId);
      } catch (BitmexQueryOrderException | IOException ignore) {
        return;
      }
      boolean longFilled = BitmexExchange.ORDER_STATUS_FILLED.equals(longOrder.getOrdStatus());
      boolean shortFilled = BitmexExchange.ORDER_STATUS_FILLED.equals(shortOrder.getOrdStatus());
      boolean longCanceled = BitmexExchange.ORDER_STATUS_CANCELED.equals(longOrder.getOrdStatus());
      boolean shortCanceled = BitmexExchange.ORDER_STATUS_CANCELED.equals(shortOrder.getOrdStatus());
      int m = 10;
      int t = 5;
      int longSnapshot = (int) (longPrice * m);
      int shortSnapshot = (int) (shortPrice * m);
      int bestBidSnapshot = (int) (bestBid * m);
      int bestAskSnapshot = (int) (bestAsk * m);
      if (longFilled && shortFilled) {
        log.info("Long order filled at {}.", longPrice);
        log.info("Short order filled at {}.", shortPrice);
        this.longOrderId = null;
        this.shortOrderId = null;
      } else if (longFilled) {
        if (longSnapshot < bestAskSnapshot && bestAskSnapshot < shortSnapshot) {
          log.info("Amend short order from {} to {}.", shortPrice, bestAsk);
          BitmexPrivateOrder order = tryAmendShortOrder(shortOrderId, bestAsk, shortCanceled);
          this.shortOrderId = order.getId();
          this.shortPrice = bestAsk;
        } else if (bestAskSnapshot <= longSnapshot && shortSnapshot - longSnapshot > t) {
          double newShortPrice = longPrice + TICK;
          log.info("Amend short order from {} to {}.", shortPrice, newShortPrice);
          BitmexPrivateOrder order = tryAmendShortOrder(shortOrderId, newShortPrice, shortCanceled);
          this.shortOrderId = order.getId();
          this.shortPrice = newShortPrice;
        } else {
          log.info("Long order filled at {}. Put short order at {} to waiting list.", longPrice, shortPrice);
          waitings.add(new OrderHistory(shortOrderId, longPrice, this.createAt, BitmexSide.SELL));
          this.longOrderId = null;
          this.shortOrderId = null;
        }
      } else if (shortFilled) {
        if (longSnapshot < bestBidSnapshot && bestBidSnapshot < shortSnapshot) {
          log.info("Amend long order from {} to {}.", longPrice, bestBid);
          BitmexPrivateOrder order = tryAmendLongOrder(longOrderId, bestBid, longCanceled);
          this.longOrderId = order.getId();
          this.longPrice = bestBid;
        } else if (shortSnapshot <= bestBidSnapshot && shortSnapshot - longSnapshot > t) {
          double newLongPrice = shortPrice - TICK;
          log.info("Amend long order from {} to {}.", longPrice, newLongPrice);
          BitmexPrivateOrder order = tryAmendLongOrder(longOrderId, newLongPrice, longCanceled);
          this.longOrderId = order.getId();
          this.longPrice = newLongPrice;
        } else {
          log.info("Short order filled at {}. Put long order at {} to waiting list.", shortPrice, longPrice);
          waitings.add(new OrderHistory(longOrderId, shortPrice, this.createAt, BitmexSide.BUY));
          this.longOrderId = null;
          this.shortOrderId = null;
        }
      } else {
        ob = deltaClient.getOrderBookL2(symbol);
        double midPrice = (bestAsk + bestBid) / 2;
        int compare = Double.compare(midPrice, lastMidPrice);
        this.lastMidPrice = midPrice;
        double imb = ob.imbalance();
        if (imb < -this.imbalance) {
          if (compare == 0 && bestBidSnapshot - t != longSnapshot) {
            log.info("Mid price does not change");
            double newLongPrice = bestBid - TICK;
            log.info("1Amend long price from {} to {}.", longPrice, newLongPrice);
            BitmexPrivateOrder order = tryAmendLongOrder(longOrderId, newLongPrice, longCanceled);
            this.longOrderId = order.getId();
            this.longPrice = newLongPrice;
            return;
          }
          double fairBid = ob.findFairBid();
          int fairBidSnapshot = (int) (fairBid * m);
          if (longSnapshot < fairBidSnapshot) {
            fairBid -= TICK;
            fairBidSnapshot -= t;
          }
          if (fairBidSnapshot != longSnapshot) {
            log.info("3Amend long order from {} to {}.", longPrice, fairBid);
            BitmexPrivateOrder order = tryAmendLongOrder(longOrderId, fairBid, longCanceled);
            this.longOrderId = order.getId();
            this.longPrice = fairBid;
          }
          if (bestAskSnapshot < shortSnapshot) {
            log.info("4Amend short order from {} to {}.", shortPrice, bestAsk);
            BitmexPrivateOrder order = tryAmendShortOrder(shortOrderId, bestAsk, shortCanceled);
            this.shortOrderId = order.getId();
            this.shortPrice = bestAsk;
          }
        } else if (imb > this.imbalance) {
          if (compare == 0 && bestAskSnapshot + t != shortSnapshot) {
            log.info("Mid price does not change");
            double newShortPrice = bestAsk + TICK;
            log.info("1Amend short price from {} to {}.", shortPrice, newShortPrice);
            BitmexPrivateOrder order = tryAmendLongOrder(shortOrderId, newShortPrice, shortCanceled);
            this.shortOrderId = order.getId();
            this.shortPrice = newShortPrice;
            return;
          }
          double fairAsk = ob.findFairAsk();
          int fairAskSnapshot = (int) (fairAsk * m);
          if (shortSnapshot > fairAskSnapshot) {
            fairAsk += TICK;
            fairAskSnapshot += t;
          }
          if (fairAskSnapshot != shortSnapshot) {
            log.info("5Amend short order from {} to {}.", shortPrice, fairAsk);
            BitmexPrivateOrder order = tryAmendShortOrder(shortOrderId, fairAsk, shortCanceled);
            this.shortOrderId = order.getId();
            this.shortPrice = fairAsk;
          }
          if (bestBidSnapshot > longSnapshot) {
            log.info("6Amend long order from {} to {}.", longPrice, bestBid);
            BitmexPrivateOrder order = tryAmendLongOrder(longOrderId, bestBid, longCanceled);
            this.longOrderId = order.getId();
            this.longPrice = bestBid;
          }
        } else {
          if (longSnapshot != bestBidSnapshot) {
            log.info("7Amend long order from {} to {}.", longPrice, bestBid);
            BitmexPrivateOrder bitmexPrivateOrder = tryAmendLongOrder(longOrderId, bestBid, longCanceled);
            this.longOrderId = bitmexPrivateOrder.getId();
            this.longPrice = bestBid;
          }
          if (shortSnapshot != bestAskSnapshot) {
            log.info("8Amend short order from {} to {}.", shortPrice, bestAsk);
            BitmexPrivateOrder bitmexPrivateOrder = tryAmendShortOrder(shortOrderId, bestAsk, shortCanceled);
            this.shortOrderId = bitmexPrivateOrder.getId();
            this.shortPrice = bestAsk;
          }
        }
      }
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
