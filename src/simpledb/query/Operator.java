package simpledb.query;

import java.util.*;

public class Operator {
  private static final HashSet<String> VALID_OPERATORS = new HashSet<>(
      Arrays.asList("=", "<", ">", "<=", ">=", "<>", "!="));
  private static final HashSet<String> INEQUALITY_OPERATORS = new HashSet<>(Arrays.asList("<", ">", "<=", ">="));
  private String opVal;

  public Operator(String opVal) {
    this.opVal = opVal;
  }

  public String getVal() {
    return this.opVal;
  }

  public boolean isInequality() {
    return INEQUALITY_OPERATORS.contains(opVal);
  }

  public static boolean isValidOpString(String opStr) {
    return VALID_OPERATORS.contains(opStr);
  }
}
