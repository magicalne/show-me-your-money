package io.magicalne.smym.exchanges.bitmex;

import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bitmex.dto.marketdata.BitmexKline;
import org.knowm.xchange.bitmex.dto.marketdata.BitmexPrivateOrder;
import org.knowm.xchange.bitmex.dto.trade.*;
import org.knowm.xchange.bitmex.service.BitmexMarketDataService;
import org.knowm.xchange.bitmex.service.BitmexTradeService;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BitmexExchange {

  private final BitmexTradeService tradeService;
  private final BitmexMarketDataService marketDataService;

  public BitmexExchange(String accessId, String secretKey) {
    ExchangeSpecification exSpec = new org.knowm.xchange.bitmex.BitmexExchange().getDefaultExchangeSpecification();
    exSpec.setApiKey(accessId);
    exSpec.setSecretKey(secretKey);
//    exSpec.setSslUri("https://testnet.bitmex.com/");
//    exSpec.setHost("testnet.bitmex.com");
    Exchange exchange = ExchangeFactory.INSTANCE.createExchange(exSpec);
    tradeService = ((BitmexTradeService) exchange.getTradeService());
    marketDataService = (BitmexMarketDataService) exchange.getMarketDataService();
  }

  public BitmexPrivateOrder placeLimitOrder(String symbol, double price, int contracts, BitmexSide side) {
    BitmexPlaceOrderParameters param = new BitmexPlaceOrderParameters.Builder(symbol)
      .setSide(side)
      .setPrice(new BigDecimal(price))
      .setOrderQuantity(new BigDecimal(contracts))
      .setOrderType(BitmexOrderType.LIMIT)
      .setExecutionInstructions(Collections.singletonList(BitmexExecutionInstruction.PARTICIPATE_DO_NOT_INITIATE))
      .build();
    return this.tradeService.placeOrder(param);
  }

  public BitmexPrivateOrder placeMarketOrder(String symbol, int contracts, BitmexSide side) {
    BitmexPlaceOrderParameters param = new BitmexPlaceOrderParameters.Builder(symbol)
      .setSide(side)
      .setOrderQuantity(new BigDecimal(contracts))
      .setOrderType(BitmexOrderType.MARKET)
      .build();
    return this.tradeService.placeOrder(param);
  }

  public List<BitmexPrivateOrder> placePairOrders(String symbol, double bidPrice, double askPrice, int contracts) {
    BitmexPlaceOrderParameters bidParam = new BitmexPlaceOrderParameters.Builder(symbol)
      .setSide(BitmexSide.BUY)
      .setPrice(new BigDecimal(bidPrice))
      .setOrderQuantity(new BigDecimal(contracts))
      .setOrderType(BitmexOrderType.LIMIT)
      .setExecutionInstructions(Collections.singletonList(BitmexExecutionInstruction.PARTICIPATE_DO_NOT_INITIATE))
      .build();
    PlaceOrderCommand bid = new PlaceOrderCommand(bidParam);
    BitmexPlaceOrderParameters askParam = new BitmexPlaceOrderParameters.Builder(symbol)
      .setSide(BitmexSide.SELL)
      .setPrice(new BigDecimal(askPrice))
      .setOrderQuantity(new BigDecimal(contracts))
      .setOrderType(BitmexOrderType.LIMIT)
      .setExecutionInstructions(Collections.singletonList(BitmexExecutionInstruction.PARTICIPATE_DO_NOT_INITIATE))
      .build();
    PlaceOrderCommand ask = new PlaceOrderCommand(askParam);

    List<PlaceOrderCommand> commands = Arrays.asList(bid, ask);
    return this.tradeService.placeOrderBulk(commands);
  }

  public List<BitmexPrivateOrder> placeOrdersBulk(List<PlaceOrderCommand> commands) {
    return this.tradeService.placeOrderBulk(commands);
  }

  public BitmexPrivateOrder amendOrderPrice(String orderId, int contracts, double price, String text) {
    BitmexReplaceOrderParameters param = new BitmexReplaceOrderParameters.Builder()
      .setOrderId(orderId)
      .setOrderQuantity(new BigDecimal(contracts))
      .setPrice(new BigDecimal(price))
      .setText(text)
      .build();
    return this.tradeService.replaceOrder(param);
  }

  public BitmexPrivateOrder amendOrderPrice(String orderId, int contracts, double price) {
    return amendOrderPrice(orderId, contracts, price, null);
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

  public List<BitmexKline> getRecentStats(String symbol) {

    return marketDataService.getBucketedTrades("1h", false, symbol, 240, true);
  }
}
