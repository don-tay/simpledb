package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * 
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private Collection<String> tables;
   private Boolean isDistinct;
   private Predicate pred;
   private List<SortField> sortfields;
   private Optional<List<String>> groupbyfields;
   private Optional<List<AggregationFn>> aggFuncs;

   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(List<String> fields, Collection<String> tables, Boolean isDistinct,
      Predicate pred, List<SortField> sortfields, Optional<List<String>> groupbyfields,
      Optional<List<AggregationFn>> aggFuncs) {
      this.fields = fields;
      this.tables = tables;
      this.isDistinct = isDistinct;
      this.pred = pred;
      this.sortfields = sortfields;
      this.groupbyfields = groupbyfields;
      this.aggFuncs = aggFuncs;
   }

   /**
    * Returns the fields mentioned in the select clause.
    * 
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }

   /**
    * Returns the tables mentioned in the from clause.
    * 
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }

   /**
    * Returns whether records in output table should be distinct.
    * 
    * @return records distinct boolean
    */
   public Boolean isDistinct() {
      return isDistinct;
   }

   /**
    * Returns the predicate that describes which records should be in the output
    * table.
    * 
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }

   /**
    * Returns the sort fields mentioned in the select clause.
    * 
    * @return a list of field names
    */
   public List<SortField> sortfields() {
      return sortfields;
   }

      /**
    * Returns the Group By fields mentioned in the Group By clause.
    * 
    * @return a list of field names
    */
    public Optional<List<String>> groupbyfields() {
      return groupbyfields;
   }

   /**
    * Returns the Group By fields mentioned in the Group By clause.
    * 
    * @return a list of field names
    */
    public Optional<List<AggregationFn>> aggregateFuncs() {
      return aggFuncs;
   }

   public String toString() {
      String result = "select ";
      if (isDistinct)
         result += "distinct ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length() - 2); // remove final comma
      if (aggFuncs.isPresent()) {
         result += ", ";
         for (AggregationFn func: aggFuncs.get()) {
            result += func.fieldName() + ", ";
         }
         result = result.substring(0, result.length() - 2); // remove final comma
      }
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length() - 2); // remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      if (groupbyfields.isPresent()) {
         result += " group by ";
         for (String groupbyfield : groupbyfields.get()) {
            result += groupbyfield.toString() + ", ";
         }
         result = result.substring(0, result.length() - 2); // remove final comma
      }
      if (!sortfields.isEmpty()) {
         result += " order by ";
         for (SortField sortfield : sortfields) {
            result += sortfield.toString() + ", ";
         }
         result = result.substring(0, result.length() - 2); // remove final comma
      }
      return result;
   }
}
