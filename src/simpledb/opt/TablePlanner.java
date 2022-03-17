package simpledb.opt;

import java.util.Map;
import java.util.Optional;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.*;
import simpledb.materialize.HashJoinPlan;
import simpledb.materialize.MergeJoinPlan;
import simpledb.index.planner.*;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.*;

/**
 * This class contains methods for planning a single table.
 * 
 * @author Edward Sciore
 */
class TablePlanner {
   private TablePlan myplan;
   private Predicate mypred;
   private Schema myschema;
   private Map<String, IndexInfo> indexes;
   private Transaction tx;
   private String tblname;

   /**
    * Creates a new table planner. The specified predicate applies to the entire
    * query. The table planner is responsible for determining which portion of the
    * predicate is useful to the table, and when indexes are useful.
    * 
    * @param tblname the name of the table
    * @param mypred  the query predicate
    * @param tx      the calling transaction
    */
   public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
      this.tblname = tblname;
      this.mypred = mypred;
      this.tx = tx;
      myplan = new TablePlan(tx, tblname, mdm);
      myschema = myplan.schema();
      indexes = mdm.getIndexInfo(tblname, tx);
   }

   /**
    * Constructs a select plan for the table. The plan will use an indexselect, if
    * possible.
    * 
    * @return a select plan for the table.
    */
   public Plan makeSelectPlan() {
      Plan p = makeIndexSelect();
      if (p == null) {
         p = myplan;
         System.out.println("Running Sequential Scan on " + tblname + " (cost=" + p.blocksAccessed() + " width="
               + p.schema().fields().size() + ")");
      }
      return addSelectPred(p);
   }

   /**
    * Constructs a join plan of the specified plan and the table. The plan will use
    * a join (nested block loop, sort merge or nested loop index), if possible. (if
    * an indexselect is also possible, the join operator takes precedence) The
    * method returns null if no join is possible.
    * 
    * @param current the specified plan
    * @return a join plan of the plan and this table
    */
   public Plan makeJoinPlan(Plan current) {
      Schema currsch = current.schema();
      Predicate joinpred = mypred.joinSubPred(myschema, currsch);
      if (joinpred == null)
         return null;
      Plan p = makeBestJoinMethod(current, currsch, joinpred);
      if (p == null)
         p = makeProductJoin(current, currsch);
      return p;
   }

   /**
    * Constructs a product plan of the specified plan and this table.
    * 
    * @param current the specified plan
    * @return a product plan of the specified plan and this table
    */
   public Plan makeProductPlan(Plan current) {
      Plan p = addSelectPred(myplan);
      return new MultibufferProductPlan(tx, current, p);
   }

   private Plan makeIndexSelect() {
      for (String fldname : indexes.keySet()) {
         Term term = mypred.equatesWithConstant(fldname);
         // create idx iff term has equality ('=') operator
         if (term != null && !term.isNonEqualOpr()) {
            Constant val = term.equatesWithConstant(fldname);
            IndexInfo ii = indexes.get(fldname);
            System.out.println("Running Index Scan using " + fldname + "(type=" + ii.indexType() + " cost="
                  + ii.blocksAccessed() + ")");
            return new IndexSelectPlan(myplan, ii, val);
         }
      }
      return null;
   }

   private Plan makeBestJoinMethod(Plan current, Schema currsch, Predicate joinpred) {
      Optional<JoinPlan> idxJoinPlan = Optional.empty();
      Optional<JoinPlan> mergeJoinPlan = Optional.empty();
      Optional<JoinPlan> hashJoinPlan = Optional.empty();

      // optimize for smaller blocksAccessed as the outer page (ie. LHS)
      Optional<JoinPlan> nestedLoopJoinPlan = (current.recordsOutput() <= myplan.recordsOutput())
            ? Optional.ofNullable(new NestedLoopsJoinPlan(current, myplan, joinpred))
            : Optional.ofNullable(new NestedLoopsJoinPlan(myplan, current, joinpred));

      // attempt to create idx and sort-merge join if no non-equal join condition eg.
      // "<>", "<=", "<"
      if (!joinpred.hasNonEqualOpr()) {
         for (String fldname : indexes.keySet()) {
            String outerfield = joinpred.equatesWithField(fldname);
            if (outerfield != null && currsch.hasField(outerfield)) {
               IndexInfo ii = indexes.get(fldname);
               idxJoinPlan = Optional.ofNullable(new IndexJoinPlan(current, myplan, ii, outerfield));
            }
         }
         for (String fldname : myschema.fields()) {
            String outerfield = joinpred.equatesWithField(fldname);
            if (outerfield != null) {
               mergeJoinPlan = Optional.ofNullable(new MergeJoinPlan(tx, current, myplan, outerfield, fldname));
               hashJoinPlan = Optional.ofNullable(new HashJoinPlan(tx, current, myplan, outerfield, fldname));
            }
         }
      }

      int idxJoinPlanCost = idxJoinPlan.isPresent() ? idxJoinPlan.get().blocksAccessed() : Integer.MAX_VALUE;
      int mergeJoinPlanCost = mergeJoinPlan.isPresent() ? mergeJoinPlan.get().blocksAccessed() : Integer.MAX_VALUE;
      int nestedLoopJoinPlanCost = nestedLoopJoinPlan.isPresent() ? nestedLoopJoinPlan.get().blocksAccessed()
            : Integer.MAX_VALUE;
      int hashJoinPlanCost = hashJoinPlan.isPresent() ? hashJoinPlan.get().blocksAccessed() : Integer.MAX_VALUE;

      JoinPlan bestplan = idxJoinPlan.orElse(null);

      if (mergeJoinPlanCost < nestedLoopJoinPlanCost && mergeJoinPlanCost < idxJoinPlanCost
            && mergeJoinPlanCost < hashJoinPlanCost) {
         bestplan = mergeJoinPlan.orElse(null);
      } else if (nestedLoopJoinPlanCost < idxJoinPlanCost && nestedLoopJoinPlanCost < mergeJoinPlanCost
            && nestedLoopJoinPlanCost < hashJoinPlanCost) {
         bestplan = nestedLoopJoinPlan.orElse(null);
      } else if (hashJoinPlanCost < nestedLoopJoinPlanCost && hashJoinPlanCost < mergeJoinPlanCost
            && hashJoinPlanCost < idxJoinPlanCost) {
         bestplan = hashJoinPlan.orElse(null);
      } else if (bestplan == null) {
         // return null when no bestplan
         return null;
      }

      bestplan.printJoinCost();

      Plan newplan = addSelectPred(bestplan);
      return addJoinPred(newplan, currsch);
   }

   private Plan makeProductJoin(Plan current, Schema currsch) {
      Plan p = makeProductPlan(current);
      System.out.println("Running cross product (cost=" + p.blocksAccessed() + ")");
      return addJoinPred(p, currsch);
   }

   private Plan addSelectPred(Plan p) {
      Predicate selectpred = mypred.selectSubPred(myschema);
      if (selectpred != null)
         return new SelectPlan(p, selectpred);
      else
         return p;
   }

   private Plan addJoinPred(Plan p, Schema currsch) {
      Predicate joinpred = mypred.joinSubPred(currsch, myschema);
      if (joinpred != null)
         return new SelectPlan(p, joinpred);
      else
         return p;
   }
}
