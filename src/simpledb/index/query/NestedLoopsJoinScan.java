package simpledb.index.query;

import simpledb.query.Constant;
import simpledb.query.Predicate;
import simpledb.query.Scan;

public class NestedLoopsJoinScan implements Scan {
   private Scan lhs, rhs;
   private Predicate joinpred;
   private boolean hasLhsVal = false;

   /**
    * Creates an nested block loop scan for the specified LHS scan and RHS scan.
    * 
    * @param lhs      the LHS scan
    * @param rhs      the RHS scan
    * @param joinpred the join predicate involved in the 2 scans
    */
   public NestedLoopsJoinScan(Scan lhs, Scan rhs, Predicate joinpred) {
      this.lhs = lhs;
      this.rhs = rhs;
      this.joinpred = joinpred;
      beforeFirst();
   }

   /**
    * Close the scan by closing the two underlying scans.
    * 
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      lhs.close();
      rhs.close();
   }

   /**
    * Position the scan before the first record, by positioning each underlying
    * scan before their first records.
    * 
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      lhs.beforeFirst();
      rhs.beforeFirst();
   }

   /**
    * Move to the next record.
    * <P>
    * Start with first LHS record. Repeatedly move the RHS scan until a common join
    * value is found. When RHS scan run out of records, move to the next LHS
    * record. When LHS runs out of records, return false.
    */
   public boolean next() {
      while (true) {
         boolean hasmore1 = true;
         if (!hasLhsVal) { // move lhs pointer when: 1. new scan 2. finished scanning rhs
            hasmore1 = lhs.next();
         }
         if (!hasmore1) {
            return false;
         }
         boolean hasmore2 = rhs.next();
         hasLhsVal = true;

         while (hasmore2) {
            if (joinpred.isSatisfied(lhs, rhs)) {
               return true;
            }

            hasmore2 = rhs.next();
         }
         // revert rhs to start if no more
         hasLhsVal = false;
         rhs.beforeFirst();
      }
   }

   /**
    * Returns the integer value of the specified field.
    * 
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public int getInt(String fldname) {
      if (rhs.hasField(fldname))
         return rhs.getInt(fldname);
      else
         return lhs.getInt(fldname);
   }

   /**
    * Returns the Constant value of the specified field.
    * 
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      if (rhs.hasField(fldname))
         return rhs.getVal(fldname);
      else
         return lhs.getVal(fldname);
   }

   /**
    * Returns the string value of the specified field.
    * 
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public String getString(String fldname) {
      if (rhs.hasField(fldname))
         return rhs.getString(fldname);
      else
         return lhs.getString(fldname);
   }

   /**
    * Returns true if the field is in the schema.
    * 
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return rhs.hasField(fldname) || lhs.hasField(fldname);
   }
}
