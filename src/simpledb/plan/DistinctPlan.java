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

/**
 * The Plan class for corresponding to the <i>distinct</i> relational algebra
 * operator. Duplicates removed.
 */
public class DistinctPlan implements Plan {
  Plan p;
  private Schema schema = new Schema();
  private Transaction tx;

  /**
   * Creates a new distinct project node in the query tree, having the specified
   * subquery and field list.
   * 
   * @param tx        the calling transaction
   * @param p         the subquery
   * @param fieldlist the list of fields
   */
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
   * The method sorts the scan and returns a scan of the sorted table scan.
   *
   * @see simpledb.plan.Plan#open()
   */
  public Scan open() {
    SortScan s = (SortScan) p.open();
    return new DistinctScan(s, schema.fields());
  }

  /**
   * Estimates the number of block accesses in the distinct projection, which is
   * the same as in the underlying query.
   * 
   * @see simpledb.plan.Plan#blocksAccessed()
   */
  public int blocksAccessed() {
    int sortingCost = p.blocksAccessed();
    Plan mp = new MaterializePlan(tx, p);
    return sortingCost + mp.blocksAccessed();
  }

  /**
   * Estimates the number of output records in the distinct projection, which is
   * the same as in the underlying query.
   * 
   * @see simpledb.plan.Plan#recordsOutput()
   */
  public int recordsOutput() {
    return p.recordsOutput();
  }

  /**
   * Estimates the number of distinct field values in the distinct projection,
   * which is the same as in the underlying query.
   * 
   * @see simpledb.plan.Plan#distinctValues(java.lang.String)
   */
  public int distinctValues(String fldname) {
    return p.distinctValues(fldname);
  }

  /**
   * Returns the schema of the projection, which is taken from the field list.
   * 
   * @see simpledb.plan.Plan#schema()
   */
  public Schema schema() {
    return p.schema();
  }

}
