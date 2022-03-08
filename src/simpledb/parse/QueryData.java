package simpledb.parse;

import java.util.*;

import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * 
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   private List<SortField> sortfields;

   /**
    * Saves the field and table list and predicate.
    */
   // TODO: Take in clause
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, List<SortField> sortfields) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.sortfields = sortfields;
   }

   // TODO: Add in clause()

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

   // TODO: Update to include for clause
   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length() - 2); // remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length() - 2); // remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
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
