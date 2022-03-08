package simpledb.parse;

import java.util.*;
import java.io.*;

/**
 * The lexical analyzer.
 * 
 * @author Edward Sciore
 */
public class Lexer {
   private Collection<String> keywords;
   private StreamTokenizer tok;
   private Collection<String> indexTypes;
   public static final String INDEX_TYPE_HASH = "hash";
   public static final String INDEX_TYPE_BTREE = "btree";

   /**
    * Creates a new lexical analyzer for SQL statement s.
    * 
    * @param s the SQL statement
    */
   public Lexer(String s) {
      initKeywords();
      initIndexType();
      tok = new StreamTokenizer(new StringReader(s));
      tok.ordinaryChar('.'); // disallow "." in identifiers
      tok.wordChars('_', '_'); // allow "_" in identifiers
      tok.lowerCaseMode(true); // ids and keywords are converted
      nextToken();
   }

   // Methods to check the status of the current token

   /**
    * Returns true if the current token is at least one of the specified delimiter
    * character(s).
    * 
    * @param d a character (or array of characters) denoting the delimiter
    * @return true if the delimiter is the current token
    */
   public boolean matchDelim(char... d) {
      for (char c : d) {
         if (c == (char) tok.ttype) {
            return true;
         }
      }
      return false;
   }

   /**
    * Returns true if the current token is an integer.
    * 
    * @return true if the current token is an integer
    */
   public boolean matchIntConstant() {
      return tok.ttype == StreamTokenizer.TT_NUMBER;
   }

   /**
    * Returns true if the current token is a string.
    * 
    * @return true if the current token is a string
    */
   public boolean matchStringConstant() {
      return '\'' == (char) tok.ttype;
   }

   /**
    * Returns true if the current token is the specified keyword.
    * 
    * @param w the keyword string
    * @return true if that keyword is the current token
    */
   public boolean matchKeyword(String w) {
      return tok.ttype == StreamTokenizer.TT_WORD && tok.sval.equals(w);
   }

   /**
    * Returns true if the current token is a legal identifier.
    * 
    * @return true if the current token is an identifier
    */
   public boolean matchId() {
      return tok.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tok.sval);
   }

   /**
    * Returns true if the current token is a legal index type.
    * 
    * @return true if the current token is an index type
    */
   public boolean matchIndexType() {
      return tok.ttype == StreamTokenizer.TT_WORD && indexTypes.contains(tok.sval);
   }

   // Methods to "eat" the current token

   /**
    * Throws an exception if the current token is not the specified delimiter.
    * Otherwise, moves to the next token.
    * 
    * @param d a character denoting the delimiter
    */
   public void eatDelim(char d) {
      if (!matchDelim(d))
         throw new BadSyntaxException();
      nextToken();
   }

   /**
    * Throws an exception if the current token is not an integer. Otherwise,
    * returns that integer and moves to the next token.
    * 
    * @return the integer value of the current token
    */
   public int eatIntConstant() {
      if (!matchIntConstant())
         throw new BadSyntaxException();
      int i = (int) tok.nval;
      nextToken();
      return i;
   }

   /**
    * Throws an exception if the current token is not a string. Otherwise, returns
    * that string and moves to the next token.
    * 
    * @return the string value of the current token
    */
   public String eatStringConstant() {
      if (!matchStringConstant())
         throw new BadSyntaxException();
      String s = tok.sval; // constants are not converted to lower case
      nextToken();
      return s;
   }

   /**
    * Throws an exception if the current token is not the specified keyword.
    * Otherwise, moves to the next token.
    * 
    * @param w the keyword string
    */
   public void eatKeyword(String w) {
      if (!matchKeyword(w))
         throw new BadSyntaxException();
      nextToken();
   }

   /**
    * Throws an exception if the current token is not an identifier. Otherwise,
    * returns the identifier string and moves to the next token.
    * 
    * @return the string value of the current token
    */
   public String eatId() {
      if (!matchId())
         throw new BadSyntaxException();
      String s = tok.sval;
      nextToken();
      return s;
   }

   /**
    * Throws an exception if the current token is not an accepted index type (hash
    * or btree). Otherwise, returns the index type and moves to the next token.
    * 
    * @return the string value of the current token
    */
   public String eatIndexType() {
      if (!matchIndexType()) {
         System.out.println(tok.sval);
         throw new BadSyntaxException();
      }
      String s = tok.sval;
      nextToken();
      return s;
   }

   private void nextToken() {
      try {
         tok.nextToken();
      } catch (IOException e) {
         throw new BadSyntaxException();
      }
   }

   private void initKeywords() {
      keywords = Arrays.asList("select", "distinct", "from", "where", "and", "insert", "into", "values", "delete",
            "update", "set", "create", "table", "int", "varchar", "view", "as", "index", "on", "using", "order", "by",
            "asc", "desc");
   }

   private void initIndexType() {
      indexTypes = Arrays.asList(INDEX_TYPE_HASH, INDEX_TYPE_BTREE);
   }
}
