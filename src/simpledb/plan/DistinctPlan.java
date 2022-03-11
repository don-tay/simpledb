package simpledb.plan;

import java.util.ArrayList;
import java.util.List;

import simpledb.materialize.SortPlan;
import simpledb.materialize.SortScan;
import simpledb.materialize.MaterializePlan;
import simpledb.query.DistinctScan;
import simpledb.query.Scan;
import simpledb.query.SortField;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class DistinctPlan implements Plan {
  Plan p;
  private Schema schema = new Schema();
  private Transaction tx;

  public DistinctPlan(Transaction tx, Plan p, List<String> fieldlist) {
    List<SortField> sortfields = new ArrayList<>();
    for (String field : fieldlist) {
      sortfields.add(new SortField(field, "asc"));
      this.schema.add(field, p.schema());
    }
    this.p = new SortPlan(tx, p, sortfields);
    this.tx = tx;
  }

  /**
   * Opens a scan corresponding to this plan. The scan will be positioned before
   * its first record.
   * 
   * @return a scan
   */
  public Scan open() {
    SortScan s = (SortScan) p.open();
    return new DistinctScan(s, schema.fields());
  }

  /**
   * Returns an estimate of the number of block accesses that will occur when the
   * scan is read to completion.
   * 
   * @return the estimated number of block accesses
   */
  public int blocksAccessed() {
    int sortingCost = p.blocksAccessed();
    Plan mp = new MaterializePlan(tx, p);
    return sortingCost + mp.blocksAccessed();
  }

  /**
   * Returns an estimate of the number of records in the query's output table.
   * 
   * @return the estimated number of output records
   */
  public int recordsOutput() {
    return p.recordsOutput();
  }

  /**
   * Returns an estimate of the number of distinct values for the specified field
   * in the query's output table.
   * 
   * @param fldname the name of a field
   * @return the estimated number of distinct field values in the output
   */
  public int distinctValues(String fldname) {
    return p.distinctValues(fldname);
  }

  /**
   * Returns the schema of the query.
   * 
   * @return the query's schema
   */
  public Schema schema() {
    return p.schema();
  }

}
