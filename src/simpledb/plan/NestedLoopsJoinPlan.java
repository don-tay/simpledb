package simpledb.plan;

import simpledb.query.NestedLoopsJoinScan;
import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class NestedLoopsJoinPlan implements Plan {
   private Plan p1, p2;
   private Predicate joinpred;
   private Schema sch = new Schema();

   /**
    * Implements the join operator, using the specified LHS and RHS plans.
    * 
    * @param p1       the left-hand plan
    * @param p2       the right-hand plan
    * @param joinpred the join predicate involved in this table
    */
   public NestedLoopsJoinPlan(Plan p1, Plan p2, Predicate joinpred) {
      this.p1 = p1;
      this.p2 = p2;
      this.joinpred = joinpred;
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
      return new NestedLoopsJoinScan(s1, s2, joinpred);
   }

   /**
    * Estimates the number of block accesses to compute the join. The formula is:
    * 
    * <pre>
    * B(nestedloopjoin(p1, p2)) = B(p1) + R(p1) * B(p2)
    * </pre>
    * 
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return p1.blocksAccessed() + (p1.recordsOutput() * p2.blocksAccessed());
   }

   /**
    * Estimates the number of output records in the join. The formula is:
    * 
    * <pre>
    * R(nestedloopsjoin(p1, p2)) = R(p1) * R(p2)
    * </pre>
    * 
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p1.recordsOutput() * p2.recordsOutput();
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
