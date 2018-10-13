package io.magicalne.smym.strategy;

import com.google.common.base.Preconditions;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class StrategyExecutor {

  /**
   *
   * @param args args[0]: classpath, args[1]: yaml config file path
   */
  public static void main(String[] args)
    throws InterruptedException,
    ClassNotFoundException,
    NoSuchMethodException,
    IllegalAccessException,
    InvocationTargetException,
    InstantiationException {
    String accessKey = System.getenv("BINANCE_ACCESS_KEY");
    String secretKey = System.getenv("BINANCE_ACCESS_SECRET_KEY");
    Preconditions.checkArgument(args.length == 2);
    String classpath = args[0];
    String yamlPath = args[1];
    Class<?> clazz = Class.forName(classpath);
    Constructor<?> constructor = clazz.getConstructor(String.class, String.class, String.class);
    Object o = constructor.newInstance(accessKey, secretKey, yamlPath);
    Method execute = o.getClass().getMethod("execute");
    execute.invoke(o);
  }
}
