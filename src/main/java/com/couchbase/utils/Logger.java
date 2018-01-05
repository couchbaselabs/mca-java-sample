package com.couchbase.utils;

import java.util.Arrays;
import java.util.HashSet;

public abstract class Logger {
  private static final HashSet<String> groups = new HashSet<>();

  public static void init(String spec) {
    if (null == spec) return;

    Arrays.stream(spec.split(",")).forEach(group -> groups.add(group.toLowerCase()));
  }

  public static void log(String group, String... args) {
    if (!groups.contains(group)) return;

    Arrays.stream(args).forEach(arg -> System.err.append(arg));
    System.err.append('\n');
  }

  public static boolean logging(String group) {
    return groups.contains(group);
  }
}