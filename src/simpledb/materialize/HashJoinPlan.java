package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.*;

import java.util.*;

/**
 * The Plan class for the <i>hashjoin</i> operator.
 * 
 * @author Edward Sciore
 */
public class HashJoinPlan implements Plan {
  private Plan p1, p2;
  private String fldname1, fldname2;
  private Schema sch = new Schema();
  private Transaction tx;
  private int hashBucketCount;

  /**
   * Creates a hashjoin plan for the two specified queries. The RHS must be
   * materialized after it is sorted, in order to deal with possible duplicates.
   * 
   * @param p1       the LHS query plan
   * @param p2       the RHS query plan
   * @param fldname1 the LHS join field
   * @param fldname2 the RHS join field
   * @param tx       the calling transaction
   */
  public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
    this.p1 = p1;
    this.p2 = p2;
    this.fldname1 = fldname1;
    this.fldname2 = fldname2;
    this.tx = tx;
    // hash into (B-1) buckets
    this.hashBucketCount = tx.availableBuffs() - 1;
    System.out.println("Avail hash buckets: " + hashBucketCount);

    sch.addAll(p1.schema());
    sch.addAll(p2.schema());
  }

  /**
   * The method first sorts its two underlying scans on their join field. It then
   * returns a hashjoin scan of the two sorted table scans.
   * 
   * @see simpledb.plan.Plan#open()
   */
  public Scan open() {
    System.out.println("Hashing 1st table...");
    List<TempTable> b1 = hashToBuckets(p1, fldname1);
    System.out.println("Hashing 2nd table...");
    List<TempTable> b2 = hashToBuckets(p2, fldname2);
    return new HashJoinScan(tx, b1, b2, fldname1, fldname2);
  }

  /**
   * Return the number of block acceses required to perform hashjoin on the
   * tables. Since a hashjoin can be preformed with a single pass through each
   * table, the method returns the sum of the block accesses of the materialized
   * tables. Page nested loop is then used to probe between the respective
   * partitions. Hence, the cost is the sum of hashing, materializing and
   * page-nested loop based probing (assuming partitions are evenly distributed.)
   * 
   * @see simpledb.plan.Plan#blocksAccessed()
   */
  public int blocksAccessed() {
    Plan mp1 = new MaterializePlan(tx, p1);
    Plan mp2 = new MaterializePlan(tx, p2);
    int hashingCost = mp1.blocksAccessed() + mp2.blocksAccessed();
    // ? Review formula
    return hashingCost + p1.blocksAccessed() + p2.blocksAccessed();
  }

  /**
   * Return the number of records in the join. Assuming uniform distribution, the
   * formula is:
   * 
   * <pre>
   *  R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}
   * </pre>
   * 
   * @see simpledb.plan.Plan#recordsOutput()
   */
  public int recordsOutput() {
    int maxvals = Math.max(p1.distinctValues(fldname1), p2.distinctValues(fldname2));
    return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
  }

  /**
   * Estimate the distinct number of field values in the join. Since the join does
   * not increase or decrease field values, the estimate is the same as in the
   * appropriate underlying query.
   * 
   * @see simpledb.plan.Plan#distinctValues(java.lang.String)
   */
  public int distinctValues(String fldname) {
    if (p1.schema().hasField(fldname))
      return p1.distinctValues(fldname);
    else
      return p2.distinctValues(fldname);
  }

  /**
   * Return the schema of the join, which is the union of the schemas of the
   * underlying queries.
   * 
   * @see simpledb.plan.Plan#schema()
   */
  public Schema schema() {
    return sch;
  }

  private List<TempTable> hashToBuckets(Plan p, String joinfield) {
    List<TempTable> tempTables = initTempTables(p);
    List<String> fields = p.schema().fields();
    Scan s = p.open();
    boolean hasmore = s.next();
    if (!hasmore) {
      return tempTables;
    }

    while (hasmore) {
      Constant c = s.getVal(joinfield);
      int idx = c.hashCode() % hashBucketCount;
      UpdateScan tblToInsert = tempTables.get(idx).open();
      tblToInsert.insert();
      System.out.print("Inserted into table " + idx + ": ");
      for (String fldname : fields) {
        System.out.print(s.getVal(fldname) + " ");
        tblToInsert.setVal(fldname, s.getVal(fldname));
      }
      System.out.println();
      tblToInsert.close();
      hasmore = s.next();
    }
    s.close();
    return tempTables;
  }

  private List<TempTable> initTempTables(Plan p) {
    List<TempTable> tempTables = new ArrayList<>();
    for (int i = 0; i < hashBucketCount; ++i) {
      tempTables.add(new TempTable(tx, p.schema()));
    }
    return tempTables;
  }
}
