package simpledb.parse;

import simpledb.query.Operator;

public class PredParser {
   private Lexer lex;

   public PredParser(String s) {
      lex = new Lexer(s);
   }

   public String field() {
      return lex.eatId();
   }

   public void constant() {
      if (lex.matchStringConstant())
         lex.eatStringConstant();
      else
         lex.eatIntConstant();
   }

   /**
    * Operator lexer
    * TODO: Refactor into Lexer logic
    */
   public void operator() {
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
   }

   public void expression() {
      if (lex.matchId())
         field();
      else 
         constant();
   }

   public void term() {
      expression();
      operator();
      expression();
   }

   public void predicate() {
      term();
      if (lex.matchKeyword("and")) {
         lex.eatKeyword("and");
         predicate();
      }
   }
}

