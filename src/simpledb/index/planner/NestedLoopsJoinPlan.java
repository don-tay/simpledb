package simpledb.index.planner;

import simpledb.index.query.NestedLoopsJoinScan;
import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class NestedLoopsJoinPlan implements Plan {
   private Plan p1, p2;
   private String joinfield;
   private Schema sch = new Schema();

   /**
    * Implements the join operator, using the specified LHS and RHS plans.
    * 
    * @param p1        the left-hand plan
    * @param p2        the right-hand plan
    * @param joinfield the left-hand field used for joining
    */
   public NestedLoopsJoinPlan(Plan p1, Plan p2, String joinfield) {
      this.p1 = p1;
      this.p2 = p2;
      this.joinfield = joinfield;
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
   }

   /**
    * Opens a nestedloopsjoin scan for this query
    * 
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan s1 = p1.open();
      Scan s2 = p2.open();
      return new NestedLoopsJoinScan(s1, joinfield, s2);
   }

   /**
    * Estimates the number of block accesses to compute the join. If block nested
    * loop is implemented, blocks accessed = p1.blocksAccessed()
    * 
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return p1.blocksAccessed();
   }

   /**
    * Estimates the number of output records in the join. The formula is:
    * 
    * <pre>
    *  
    * TODO: FILL IN FORMULA
    * </pre>
    * 
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      // TODO: calculation
      return 0;
   }

   /**
    * Estimates the number of distinct values for the specified field.
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
    * Return the schema of the join.
    * 
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
}
