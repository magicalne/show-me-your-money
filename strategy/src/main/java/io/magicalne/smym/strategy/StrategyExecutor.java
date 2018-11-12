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
    throws
    ClassNotFoundException,
    NoSuchMethodException,
    IllegalAccessException,
    InvocationTargetException,
    InstantiationException {
    Preconditions.checkArgument(args.length == 2);
    String classpath = args[0];
    String yamlPath = args[1];
    Class<?> clazz = Class.forName(classpath);
    Constructor<?> constructor = clazz.getConstructor(String.class);
    Object o = constructor.newInstance(yamlPath);
    Method execute = o.getClass().getMethod("execute");
    execute.invoke(o);
  }
}
