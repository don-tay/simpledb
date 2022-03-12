package simpledb.materialize;

import java.util.List;

import simpledb.query.*;
import simpledb.tx.Transaction;

/**
 * The Scan class for the <i>hashjoin</i> operator.
 * 
 * @author Edward Sciore
 */
public class HashJoinScan implements Scan {
   private Transaction tx;
   private List<TempTable> b1, b2;
   private UpdateScan s1, s2;
   private String fldname1, fldname2;
   private int currIdx = 0;
   private Constant joinval;

   /**
    * Create a hashjoin scan for the two underlying hash buckets.
    * 
    * @param b1       the LHS hashed buckets
    * @param b2       the RHS hashed buckets
    * @param fldname1 the LHS join field
    * @param fldname2 the RHS join field
    * @param tx       the current transaction
    */
   public HashJoinScan(Transaction tx, List<TempTable> b1, List<TempTable> b2, String fldname1, String fldname2) {
      this.tx = tx;
      this.b1 = b1;
      this.b2 = b2;
      this.fldname1 = fldname1;
      this.fldname2 = fldname2;
      beforeFirst();
   }

   /**
    * Close the scan by closing the two underlying scans.
    * 
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      s1.close();
      s2.close();
   }

   /**
    * Position the scan before the first record, by positioning each underlying
    * scan before their first records.
    * 
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      this.s1 = b1.get(currIdx).open();
      this.s2 = b2.get(currIdx).open();
      s1.beforeFirst();
      s2.beforeFirst();
   }

   /**
    * Move to the next record.
    * <P>
    * Start with first LHS record. Repeatedly move the RHS scan until a common join
    * value is found. When RHS scan run out of records, move to the next LHS
    * record. When LHS runs out of records, return false.
    */
   public boolean next() {
      boolean hasmore1 = true;
      // TODO: bug in s1 traversal
      while (true) {
         if (joinval == null) { // move lhs pointer when: 1. new scan 2. finished scanning rhs
            hasmore1 = s1.next();
         }
         if (!hasmore1) { // if lhs has no more
            close(); // close scan on current table
            joinval = null;
            if (++currIdx >= b1.size()) { // end scan if no more temptables
               return false;
            }
            beforeFirst();
            return next(); // recursive call to scan new temptable
         }
         Constant v1 = s1.getVal(fldname1);
         boolean hasmore2 = s2.next();

         while (hasmore2) {
            Constant v2 = s2.getVal(fldname2);
            if (v1.compareTo(v2) == 0) {
               joinval = s1.getVal(fldname1);
               return true;
            }

            hasmore2 = s2.next();
         }
         // revert rhs to start if no more
         joinval = null;
         s2.beforeFirst();
      }
   }

   /**
    * Return the integer value of the specified field. The value is obtained from
    * whichever scan contains the field.
    * 
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
   public int getInt(String fldname) {
      if (s1.hasField(fldname))
         return s1.getInt(fldname);
      else
         return s2.getInt(fldname);
   }

   /**
    * Return the string value of the specified field. The value is obtained from
    * whichever scan contains the field.
    * 
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
   public String getString(String fldname) {
      if (s1.hasField(fldname))
         return s1.getString(fldname);
      else
         return s2.getString(fldname);
   }

   /**
    * Return the value of the specified field. The value is obtained from whichever
    * scan contains the field.
    * 
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      if (s1.hasField(fldname))
         return s1.getVal(fldname);
      else
         return s2.getVal(fldname);
   }

   /**
    * Return true if the specified field is in either of the underlying scans.
    * 
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return s1.hasField(fldname) || s2.hasField(fldname);
   }
}
