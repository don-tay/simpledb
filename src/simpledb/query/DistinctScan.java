package simpledb.query;

import java.util.ArrayList;
import java.util.List;

import simpledb.materialize.SortScan;

/**
 * The Scan class for the <i>project</i> operator which removes duplicate
 * records.
 */
public class DistinctScan implements Scan {
  private SortScan s;
  private List<String> fieldlist;
  private List<Constant> previousRecord;

  public DistinctScan(SortScan s, List<String> fieldlist) {
    this.s = s;
    this.fieldlist = fieldlist;
    this.previousRecord = new ArrayList<>();
    beforeFirst();
  }

  /**
   * Position the scan before its first record. A subsequent call to next() will
   * return the first record.
   */
  public void beforeFirst() {
    s.beforeFirst();
  }

  /**
   * Move to the next record. If the record has appeared before, move the scan
   * until the record is unique. When the scan runs out of records, return false.
   * 
   * @return false if there is no next record
   */
  public boolean next() {
    boolean hasmore = s.next();
    while (hasmore) {
      if (previousRecord.isEmpty()) {
        for (String field : fieldlist) {
          previousRecord.add(s.getVal(field));
        }
        return true;
      } else {
        for (int i = 0; i < fieldlist.size(); i++) {
          String field = fieldlist.get(i);
          Constant value = s.getVal(field);
          Constant prevRecVal = previousRecord.get(i);
          if (value.compareTo(prevRecVal) != 0) {
            for (int j = 0; j < fieldlist.size(); j++) {
              String field2 = fieldlist.get(j);
              previousRecord.set(i, s.getVal(field2));
            }
            return true;
          }
        }
        hasmore = s.next();
      }
    }
    return false;
  }

  /**
   * Return the value of the specified integer field in the current record.
   * 
   * @param fldname the name of the field
   * @return the field's integer value in the current record
   */
  public int getInt(String fldname) {
    if (hasField(fldname))
      return s.getInt(fldname);
    else
      throw new RuntimeException("field " + fldname + " not found.");
  }

  /**
   * Return the value of the specified string field in the current record.
   * 
   * @param fldname the name of the field
   * @return the field's string value in the current record
   */
  public String getString(String fldname) {
    if (hasField(fldname))
      return s.getString(fldname);
    else
      throw new RuntimeException("field " + fldname + " not found.");
  }
  /**
   * Return the value of the specified field in the current record. The value is
   * expressed as a Constant.
   * 
   * @param fldname the name of the field
   * @return the value of that field, expressed as a Constant.
   */
  public Constant getVal(String fldname) {
    if (hasField(fldname))
      return s.getVal(fldname);
    else
      throw new RuntimeException("field " + fldname + " not found.");
  }

  /**
   * Return true if the scan has the specified field.
   * 
   * @param fldname the name of the field
   * @return true if the scan has that field
   */
  public boolean hasField(String fldname) {
    return fieldlist.contains(fldname);
  }

  /**
   * Close the scan and its subscans, if any.
   */
  public void close() {
    s.close();
  }
}
