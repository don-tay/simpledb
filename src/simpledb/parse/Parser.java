package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.materialize.CountFn;
import simpledb.materialize.MaxFn;
import simpledb.query.*;
import simpledb.record.*;

/**
 * The SimpleDB parser.
 * 
 * @author Edward Sciore
 */
public class Parser {
   private Lexer lex;

   public Parser(String s) {
      lex = new Lexer(s);
   }

   // Methods for parsing predicates, terms, expressions, constants, and fields

   public String field() {
      return lex.eatId();
   }

   public Constant constant() {
      if (lex.matchStringConstant())
         return new Constant(lex.eatStringConstant());
      else
         return new Constant(lex.eatIntConstant());
   }

   public Expression expression() {
      if (lex.matchId())
         return new Expression(field());
      else
         return new Expression(constant());
   }

   /**
    * Operator lexer 
    * TODO: Refactor into Lexer logic
    */
   public Operator operator() {
      String opStr = "";
      while (lex.matchDelim('=', '!', '<', '>')) {
         if (lex.matchDelim('=')) {
            lex.eatDelim('=');
            opStr += "=";
         } else if (lex.matchDelim('!')) {
            lex.eatDelim('!');
            opStr += "!";
         } else if (lex.matchDelim('<')) {
            lex.eatDelim('<');
            opStr += "<";
         } else if (lex.matchDelim('>')) {
            lex.eatDelim('>');
            opStr += ">";
         }
      }

      // throw error if invalid operator
      if (!Operator.isValidOpString(opStr)) {
         throw new BadSyntaxException();
      }

      return new Operator(opStr);
   }

   public Term term() {
      Expression lhs = expression();
      Operator op = operator();
      Expression rhs = expression();
      return new Term(lhs, rhs, op);
   }

   public Predicate predicate() {
      Predicate pred = new Predicate(term());
      if (lex.matchKeyword("and")) {
         lex.eatKeyword("and");
         pred.conjoinWith(predicate());
      }
      return pred;
   }

   // Methods for parsing queries

   public QueryData query() {
      lex.eatKeyword("select");
      Boolean isDistinct = false;
      if (lex.matchKeyword("distinct")) {
         lex.eatKeyword("distinct");
         isDistinct = true;
      }
      // Parse fields and agg funcs
      List<String> fields = new ArrayList<>();
      List<AggregationFn> aggFuncs = new ArrayList<>();
      while (!lex.matchKeyword("from")) {
         if (lex.matchAggType()) {
            String currFunction = lex.eatAggType();
            lex.eatDelim('(');
            String currField = field();
            AggregationFn temp = genAggregateFunction(currField, currFunction);
            aggFuncs.add(temp);
            fields.add(temp.fieldName());
            lex.eatDelim(')');
         } else {
            fields.add(field());
         }
         if (lex.matchDelim(',')) {
            lex.eatDelim(',');
         }
      }

      lex.eatKeyword("from");
      Collection<String> tables = tableList();
      Predicate pred = new Predicate();
      List<SortField> sortfields = new ArrayList<>();
      List<String> groupbyfields = new ArrayList<>();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      if (lex.matchKeyword("group")) {
         lex.eatKeyword("group");
         lex.eatKeyword("by");
         groupbyfields = selectList();
      }
      if (lex.matchKeyword("order")) {
         lex.eatKeyword("order");
         lex.eatKeyword("by");
         sortfields = sortList();
      }
      return new QueryData(fields, tables, isDistinct, pred, sortfields, groupbyfields, aggFuncs);
   }

   private List<String> selectList() {
      List<String> L = new ArrayList<String>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(selectList());
      }
      return L;
   }

   private List<SortField> sortList() {
      List<SortField> L = new ArrayList<>();
      String field = field();
      String order = "asc";
      if (lex.matchKeyword("asc")) {
         lex.eatKeyword("asc");
      } else if (lex.matchKeyword("desc")) {
         lex.eatKeyword("desc");
         order = "desc";
      }
      L.add(new SortField(field, order));
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(sortList());
      }
      return L;
   }

   private Collection<String> tableList() {
      Collection<String> L = new ArrayList<String>();
      L.add(lex.eatId());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(tableList());
      }
      return L;
   }

   private AggregationFn genAggregateFunction(String field, String function) {
      switch (function) {
         case "count":
            return new CountFn(field);
         case "max":
            return new MaxFn(field);
         default:
            return null;
      }
   }

   // Methods for parsing the various update commands

   public Object updateCmd() {
      if (lex.matchKeyword("insert"))
         return insert();
      else if (lex.matchKeyword("delete"))
         return delete();
      else if (lex.matchKeyword("update"))
         return modify();
      else
         return create();
   }

   private Object create() {
      lex.eatKeyword("create");
      if (lex.matchKeyword("table"))
         return createTable();
      else if (lex.matchKeyword("view"))
         return createView();
      else
         return createIndex();
   }

   // Method for parsing delete commands

   public DeleteData delete() {
      lex.eatKeyword("delete");
      lex.eatKeyword("from");
      String tblname = lex.eatId();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new DeleteData(tblname, pred);
   }

   // Methods for parsing insert commands

   public InsertData insert() {
      lex.eatKeyword("insert");
      lex.eatKeyword("into");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      List<String> flds = fieldList();
      lex.eatDelim(')');
      lex.eatKeyword("values");
      lex.eatDelim('(');
      List<Constant> vals = constList();
      lex.eatDelim(')');
      return new InsertData(tblname, flds, vals);
   }

   private List<String> fieldList() {
      List<String> L = new ArrayList<String>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(fieldList());
      }
      return L;
   }

   private List<Constant> constList() {
      List<Constant> L = new ArrayList<Constant>();
      L.add(constant());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(constList());
      }
      return L;
   }

   // Method for parsing modify commands

   public ModifyData modify() {
      lex.eatKeyword("update");
      String tblname = lex.eatId();
      lex.eatKeyword("set");
      String fldname = field();
      lex.eatDelim('=');
      Expression newval = expression();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new ModifyData(tblname, fldname, newval, pred);
   }

   // Method for parsing create table commands

   public CreateTableData createTable() {
      lex.eatKeyword("table");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      Schema sch = fieldDefs();
      lex.eatDelim(')');
      return new CreateTableData(tblname, sch);
   }

   private Schema fieldDefs() {
      Schema schema = fieldDef();
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         Schema schema2 = fieldDefs();
         schema.addAll(schema2);
      }
      return schema;
   }

   private Schema fieldDef() {
      String fldname = field();
      return fieldType(fldname);
   }

   private Schema fieldType(String fldname) {
      Schema schema = new Schema();
      if (lex.matchKeyword("int")) {
         lex.eatKeyword("int");
         schema.addIntField(fldname);
      } else {
         lex.eatKeyword("varchar");
         lex.eatDelim('(');
         int strLen = lex.eatIntConstant();
         lex.eatDelim(')');
         schema.addStringField(fldname, strLen);
      }
      return schema;
   }

   // Method for parsing create view commands

   public CreateViewData createView() {
      lex.eatKeyword("view");
      String viewname = lex.eatId();
      lex.eatKeyword("as");
      QueryData qd = query();
      return new CreateViewData(viewname, qd);
   }

   // Method for parsing create index commands

   public CreateIndexData createIndex() {
      lex.eatKeyword("index");
      String idxname = lex.eatId();
      lex.eatKeyword("on");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      String fldname = field();
      lex.eatDelim(')');
      lex.eatKeyword("using");
      String indextype = lex.eatIndexType();
      return new CreateIndexData(idxname, tblname, fldname, indextype);
   }
}
