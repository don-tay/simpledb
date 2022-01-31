package simpledb.query;

import java.util.*;

public class Operator {
  private static final HashSet<String> VALID_OPERATORS_ARR = new HashSet<>(Arrays.asList("=", "<", ">", "<=", ">=", "<>", "!="));
  private String opVal;
  
  public Operator(String opVal) {
    this.opVal = opVal;
  }

  public String getVal() {
    return this.opVal;
  }

  public static boolean isValidOpString(String opStr) {
    return VALID_OPERATORS_ARR.contains(opStr);
  }
}
