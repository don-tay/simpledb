package simpledb.plan;

import simpledb.query.DistinctScan;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class DistinctPlan implements Plan {
  Plan p;

  // TODO: modify paramters if more info is needed from QueryPlanners
  // (BasicQueryPlanner, HeuristicQueryPlanner)
  public DistinctPlan(Plan p) {
    this.p = p;

  }

  /**
   * Opens a scan corresponding to this plan. The scan will be positioned before
   * its first record.
   * 
   * @return a scan
   */
  public Scan open() {
    Scan s = p.open();
    return new DistinctScan(s);
  }

  /**
   * Returns an estimate of the number of block accesses that will occur when the
   * scan is read to completion.
   * 
   * @return the estimated number of block accesses
   */
  public int blocksAccessed() {
    // TODO: Fill in appropriately
    return 0;
  }

  /**
   * Returns an estimate of the number of records in the query's output table.
   * 
   * @return the estimated number of output records
   */
  public int recordsOutput() {
    // TODO: Fill in appropriately
    return 0;
  }

  /**
   * Returns an estimate of the number of distinct values for the specified field
   * in the query's output table.
   * 
   * @param fldname the name of a field
   * @return the estimated number of distinct field values in the output
   */
  public int distinctValues(String fldname) {
    // TODO: Fill in appropriately
    return 0;
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
