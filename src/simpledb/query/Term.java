package simpledb.query;

import simpledb.plan.Plan;
import simpledb.record.*;

/**
 * A term is a comparison between two expressions.
 * 
 * @author Edward Sciore
 */
public class Term {
   private Expression lhs, rhs;
   private Operator op;

   /**
    * Create a new term that compares two expressions for equality.
    * 
    * @param lhs the LHS expression
    * @param rhs the RHS expression
    */
   public Term(Expression lhs, Expression rhs, Operator op) {
      this.lhs = lhs;
      this.rhs = rhs;
      this.op = op;
   }

   /**
    * Return true if both of the term's expressions evaluate to the same constant,
    * with respect to the specified scan.
    * 
    * @param s the scan
    * @return true if both expressions have the same value in the scan
    */
   public boolean isSatisfied(Scan s) {
      Constant lhsval = lhs.evaluate(s);
      Constant rhsval = rhs.evaluate(s);
      return isTermSatisfied(lhsval, rhsval);
   }

   /**
    * Return true if the term's expressions belong to 2 separate scans and evaluate
    * to the same constant, in their respective scans.
    * 
    * @param s1 the 1st scan
    * @param s2 the 2nd scan
    * @return true if both expressions have the same value in the scan
    */
   public boolean isSatisfied(Scan s1, Scan s2) {
      Constant lhsval, rhsval;
      if (s1.hasField(lhs.asFieldName())) {
         lhsval = lhs.evaluate(s1);
         rhsval = rhs.evaluate(s2);
      } else {
         lhsval = lhs.evaluate(s2);
         rhsval = rhs.evaluate(s1);
      }
      return isTermSatisfied(lhsval, rhsval);
   }

   /**
    * Helper method evaluating lhs and rhs constants with the term's opr
    * 
    * @param lhsval lhs constant
    * @param rhsval rhs constant
    * @return
    */
   private boolean isTermSatisfied(Constant lhsval, Constant rhsval) {
      String opval = op.getVal();
      switch (opval) {
      case "=":
         return rhsval.equals(lhsval);
      case "!=":
      case "<>":
         return !rhsval.equals(lhsval);
      case "<":
         return lhsval.compareTo(rhsval) < 0;
      case ">":
         return lhsval.compareTo(rhsval) > 0;
      case "<=":
         return lhsval.compareTo(rhsval) <= 0;
      case ">=":
         return lhsval.compareTo(rhsval) >= 0;
      default:
         throw new RuntimeException("Unknown term: " + lhsval + " " + opval + " " + rhsval);
      }
   }

   public boolean isNonEqualOpr() {
      return op.isNonEqualOpr();
   }

   /**
    * Calculate the extent to which selecting on the term reduces the number of
    * records output by a query. For example if the reduction factor is 2, then the
    * term cuts the size of the output in half.
    * 
    * @param p the query's plan
    * @return the integer reduction factor.
    */
   public int reductionFactor(Plan p) {
      String lhsName, rhsName;
      if (lhs.isFieldName() && rhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         rhsName = rhs.asFieldName();
         return Math.max(p.distinctValues(lhsName), p.distinctValues(rhsName));
      }
      if (lhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         return p.distinctValues(lhsName);
      }
      if (rhs.isFieldName()) {
         rhsName = rhs.asFieldName();
         return p.distinctValues(rhsName);
      }
      // otherwise, the term equates constants
      if (lhs.asConstant().equals(rhs.asConstant()))
         return 1;
      else
         return Integer.MAX_VALUE;
   }

   /**
    * Determine if this term is of the form "F=c" where F is the specified field
    * and c is some constant. If so, the method returns that constant. If not, the
    * method returns null.
    * 
    * @param fldname the name of the field
    * @return either the constant or null
    */
   public Constant equatesWithConstant(String fldname) {
      if (lhs.isFieldName() && lhs.asFieldName().equals(fldname) && !rhs.isFieldName())
         return rhs.asConstant();
      else if (rhs.isFieldName() && rhs.asFieldName().equals(fldname) && !lhs.isFieldName())
         return lhs.asConstant();
      else
         return null;
   }

   /**
    * Determine if this term is of the form "F1=F2" where F1 is the specified field
    * and F2 is another field. If so, the method returns the name of that field. If
    * not, the method returns null.
    * 
    * @param fldname the name of the field
    * @return either the name of the other field, or null
    */
   public String equatesWithField(String fldname) {
      if (lhs.isFieldName() && lhs.asFieldName().equals(fldname) && rhs.isFieldName())
         return rhs.asFieldName();
      else if (rhs.isFieldName() && rhs.asFieldName().equals(fldname) && lhs.isFieldName())
         return lhs.asFieldName();
      else
         return null;
   }

   /**
    * Return true if both of the term's expressions apply to the specified schema.
    * 
    * @param sch the schema
    * @return true if both expressions apply to the schema
    */
   public boolean appliesTo(Schema sch) {
      return lhs.appliesTo(sch) && rhs.appliesTo(sch);
   }

   public String toString() {
      return lhs.toString() + op.getVal() + rhs.toString();
   }
}
