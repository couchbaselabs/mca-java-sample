package com.couchbase.utils;

import static java.util.Arrays.asList;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.io.IOException;

public class Options {
  private OptionParser optionParser;
  private OptionSet optionSet;

  private Options() { throw new AssertionError("This is not the constructor you are looking for."); }

  public Options(String... arguments) {
    optionParser = new OptionParser();

    optionParser.acceptsAll(asList("b", "bucket"))
        .withRequiredArg()
        .defaultsTo("default");
    optionParser.acceptsAll(asList("c", "cluster"))
        .withRequiredArg()
        .defaultsTo("localhost");

    optionParser.acceptsAll(asList("i", "user"))
        .withRequiredArg()
        .defaultsTo("user");
    optionParser.acceptsAll(asList("p", "password"))
        .withRequiredArg()
        .defaultsTo("password");

    optionParser.acceptsAll(asList("r", "readers"))
        .withRequiredArg()
        .ofType(Integer.class)
        .defaultsTo(0);
    optionParser.acceptsAll(asList("w", "writers"))
        .withRequiredArg()
        .ofType(Integer.class)
        .defaultsTo(0);
    optionParser.acceptsAll(asList("u", "updaters"))
        .withRequiredArg()
        .ofType(Integer.class)
        .defaultsTo(0);
    optionParser.acceptsAll(asList("q", "queries"))
        .withRequiredArg()
        .ofType(Integer.class)
        .defaultsTo(0);

    optionParser.acceptsAll(asList("v", "verbose"));

    optionParser.acceptsAll(asList("h", "help"), "Display help/usage information")
        .forHelp();

    optionSet = optionParser.parse(arguments);
  }

  public void printHelp() {
    try {
      optionParser.printHelpOn(System.out);
    } catch (IOException ex) {
      System.err.println("Error printing usage - " + ex);
    }
  }

  public boolean has(String option) {
    return optionSet.has(option);
  }

  public boolean hasArgument(String option) {
    return optionSet.hasArgument(option);
  }

  public Object valueOf(String option) {
    return optionSet.valueOf(option);
  }

  public <T> T valueOf(String option, Class<T> clazz) {
    return clazz.cast(optionSet.valueOf(option));
  }

  public Integer integerValueOf(String option) {
    return valueOf(option, Integer.class);
  }

  public String stringValueOf(String option) {
    return valueOf(option, String.class);
  }
}
