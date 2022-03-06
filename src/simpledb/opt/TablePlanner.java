package simpledb.opt;

import java.util.Map;
import java.util.Optional;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.*;
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
      if (p == null)
         p = myplan;
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
         Constant val = mypred.equatesWithConstant(fldname);
         if (val != null) {
            IndexInfo ii = indexes.get(fldname);
            System.out.println("index on " + fldname + " used");
            return new IndexSelectPlan(myplan, ii, val);
         }
      }
      return null;
   }

   private Plan makeBestJoinMethod(Plan current, Schema currsch, Predicate joinpred) {
      Optional<Plan> p1 = Optional.empty();
      Optional<Plan> p2 = Optional.empty();
      Optional<Plan> p3 = Optional.empty();

      for (String fldname : indexes.keySet()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            IndexInfo ii = indexes.get(fldname);
            p1 = Optional.ofNullable(new IndexJoinPlan(current, myplan, ii, outerfield));
         }
      }
      for (String fldname : myschema.fields()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null) {
            p2 = Optional.ofNullable(new MergeJoinPlan(tx, current, myplan, outerfield, fldname));
         }
         if (outerfield != null && currsch.hasField(outerfield)) {
            // TODO: optimize for smaller blocksAccessed as the outer page (ie. LHS)
            // TODO 2: handle inequality join
            p3 = Optional.ofNullable(new NestedLoopsJoinPlan(current, myplan, outerfield, fldname));
         }
      }

      int p1Cost = p1.isPresent() ? p1.get().blocksAccessed() : Integer.MAX_VALUE;
      int p2Cost = p2.isPresent() ? p2.get().blocksAccessed() : Integer.MAX_VALUE;
      int p3Cost = p3.isPresent() ? p3.get().blocksAccessed() : Integer.MAX_VALUE;

      Plan bestplan = p1.orElse(null);

      if (p2Cost < p3Cost && p1Cost >= p2Cost) {
         System.out.println("Running sort merge");
         bestplan = p2.orElse(null);
      } else if ((p1Cost < p2Cost && p1Cost >= p3Cost) || (p2Cost < p1Cost && p2Cost >= p3Cost)) {
         System.out.println("Running nested loop join");
         bestplan = p3.orElse(null);
      } else {
         System.out.println("Running index join");
      }

      if (bestplan == null) {
         return null;
      }

      bestplan = addSelectPred(bestplan);
      return addJoinPred(bestplan, currsch);
   }

   private Plan makeProductJoin(Plan current, Schema currsch) {
      Plan p = makeProductPlan(current);
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
