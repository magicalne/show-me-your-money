package io.magicalne.smym.exchanges.bitmex;

import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bitmex.dto.marketdata.BitmexPrivateOrder;
import org.knowm.xchange.bitmex.dto.trade.*;
import org.knowm.xchange.bitmex.service.BitmexTradeService;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.dto.trade.MarketOrder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BitmexExchange {

  public static final String ORDER_STATUS_FILLED = "Filled";
  public static final String ORDER_STATUS_CANCELED = "Canceled";

  private final BitmexTradeService tradeService;

  public BitmexExchange(String accessId, String secretKey) {
    ExchangeSpecification exSpec = new org.knowm.xchange.bitmex.BitmexExchange().getDefaultExchangeSpecification();
    exSpec.setApiKey(accessId);
    exSpec.setSecretKey(secretKey);
//    exSpec.setSslUri("https://testnet.bitmex.com/");
//    exSpec.setHost("testnet.bitmex.com");
    Exchange exchange = ExchangeFactory.INSTANCE.createExchange(exSpec);
    tradeService = ((BitmexTradeService) exchange.getTradeService());
  }

  public String placeLimitLongOrder(CurrencyPair pair, double price, int contracts) {
    LimitOrder limitOrder = new LimitOrder(Order.OrderType.BID, new BigDecimal(contracts), pair, null, null, new BigDecimal(price));
    return this.tradeService.placeLimitOrder(limitOrder);
  }

  public String placeLimitShortOrder(CurrencyPair pair, double price, int contracts) {
    LimitOrder limitOrder = new LimitOrder(Order.OrderType.ASK, new BigDecimal(contracts), pair, null, null, new BigDecimal(price));
    return this.tradeService.placeLimitOrder(limitOrder);
  }

  public String placeMarketLongOrder(CurrencyPair pair, int contracts) {
    MarketOrder marketOrder = new MarketOrder(Order.OrderType.BID, new BigDecimal(contracts), pair);
    return this.tradeService.placeMarketOrder(marketOrder);
  }

  public String placeMarketShortOrder(CurrencyPair pair, int contracts) {
    MarketOrder marketOrder = new MarketOrder(Order.OrderType.ASK, new BigDecimal(contracts), pair);
    return this.tradeService.placeMarketOrder(marketOrder);
  }

  public List<BitmexPrivateOrder> placePairOrders(String symbol, double bidPrice, double askPrice, int contracts) {
    BitmexPlaceOrderParameters bidParam = new BitmexPlaceOrderParameters.Builder(symbol)
      .setSide(BitmexSide.BUY)
      .setPrice(new BigDecimal(bidPrice))
      .setOrderQuantity(new BigDecimal(contracts))
      .setOrderType(BitmexOrderType.LIMIT)
      .build();
    PlaceOrderCommand bid = new PlaceOrderCommand(bidParam);
    BitmexPlaceOrderParameters askParam = new BitmexPlaceOrderParameters.Builder(symbol)
      .setSide(BitmexSide.SELL)
      .setPrice(new BigDecimal(askPrice))
      .setOrderQuantity(new BigDecimal(contracts))
      .setOrderType(BitmexOrderType.LIMIT)
      .build();
    PlaceOrderCommand ask = new PlaceOrderCommand(askParam);

    List<PlaceOrderCommand> commands = Arrays.asList(bid, ask);
    return this.tradeService.placeOrderBulk(commands);
  }

  public List<BitmexPrivateOrder> placeOrdersBulk(String symbol, BitmexSide side, List<Double> bidPrices,
                                                  List<Integer> contracts) {
    List<PlaceOrderCommand> commands = new ArrayList<>(bidPrices.size());
    for (int i = 0; i < bidPrices.size(); i ++) {
      BitmexPlaceOrderParameters param = new BitmexPlaceOrderParameters.Builder(symbol)
        .setSide(side)
        .setPrice(new BigDecimal(bidPrices.get(i)))
        .setOrderQuantity(new BigDecimal(contracts.get(i)))
        .setOrderType(BitmexOrderType.LIMIT)
        .build();
      PlaceOrderCommand command = new PlaceOrderCommand(param);
      commands.add(command);
    }
    return this.tradeService.placeOrderBulk(commands);
  }

  public BitmexPrivateOrder amendOrderPrice(String orderId, int contracts, double price) {
    BitmexReplaceOrderParameters param = new BitmexReplaceOrderParameters.Builder()
      .setOrderId(orderId)
      .setOrderQuantity(new BigDecimal(contracts))
      .setPrice(new BigDecimal(price))
      .build();
    return this.tradeService.replaceOrder(param);
  }

  public List<BitmexPrivateOrder> amendPairOrder(String longOrderId, double bidPrice,
                                                 String shortOrderId, double askPrice, int contracts) {
    BigDecimal orderQuantity = new BigDecimal(contracts);
    BitmexReplaceOrderParameters bidParam = new BitmexReplaceOrderParameters.Builder()
      .setOrderId(longOrderId)
      .setPrice(new BigDecimal(bidPrice))
      .setOrderQuantity(orderQuantity)
      .build();
    ReplaceOrderCommand bid = new ReplaceOrderCommand(bidParam);
    BitmexReplaceOrderParameters askParam = new BitmexReplaceOrderParameters.Builder()
      .setOrderId(shortOrderId)
      .setPrice(new BigDecimal(askPrice))
      .setOrderQuantity(orderQuantity)
      .build();
    ReplaceOrderCommand ask = new ReplaceOrderCommand(askParam);
    List<ReplaceOrderCommand> pair = Arrays.asList(bid, ask);
    return this.tradeService.replaceOrderBulk(pair);
  }

  public boolean cancel(String orderId) {
    return this.tradeService.cancelOrder(orderId);
  }

  public BitmexPosition setLeverage(String symbol, double leverage) {
    return this.tradeService.updateLeveragePosition(symbol, new BigDecimal(leverage));
  }
}
