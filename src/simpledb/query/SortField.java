package simpledb.query;

public class SortField {
  private String field;
  private String order = "asc"; // default to asc when not specified

  public SortField(String field, String order) {
    this.field = field;
    this.order = order;
  }

  public String getField() {
    return field;
  }

  public String getOrder() {
    return order;
  }

  public String toString() {
    return field + " " + order;
  }
}
