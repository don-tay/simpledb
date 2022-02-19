package simpledb.index.query;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class NestedLoopsJoinScan implements Scan {
   private Scan lhs, rhs;
   private String joinfield;

   /**
    * Creates an index join scan for the specified LHS scan and RHS index.
    * 
    * @param lhs       the LHS scan
    * @param idx       the RHS index
    * @param joinfield the LHS field used for joining
    * @param rhs       the RHS scan
    */
   public NestedLoopsJoinScan(Scan lhs, String joinfield, Scan rhs) {
      this.lhs = lhs;
      this.rhs = rhs;
      this.joinfield = joinfield;
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
      // TODO: Implement method
      return false;
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
