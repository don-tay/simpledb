package test;

import java.io.File;

import simpledb.plan.Planner;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class CreateStudentDB {

   private static void deleteDbDirFiles(File dbDir) {
      for (File subfile : dbDir.listFiles()) {
         if (subfile.isDirectory()) {
            deleteDbDirFiles(dbDir);
         }
         subfile.delete();
      }
   }

   public static void main(String[] args) {
      try {
         File dbDir = new File("studentdb");
         // delete db directory if exists
         if (dbDir.exists()) {
            deleteDbDirFiles(dbDir);
            dbDir.delete();
         }

         // analogous to the driver
         SimpleDB db = new SimpleDB("studentdb");

         // analogous to the connection
         Transaction tx = db.newTx();
         Planner planner = db.planner();

         String s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";

         int numRecordsUpdated = planner.executeUpdate(s, tx);

         System.out.println("Table STUDENT created.");

         s = "create index stu_sid_idx on student(sid) using btree";

         numRecordsUpdated = planner.executeUpdate(s, tx);

         System.out.println("BTree index stu_sid_idx created on STUDENT(sid).");

         s = "create index stu_majorid_idx on student(majorid) using hash";

         numRecordsUpdated = planner.executeUpdate(s, tx);

         System.out.println("Hash index stu_majorid_idx created on STUDENT(majorid).");

         s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
         String[] studvals = { "(1, 'joe', 10, 2021)", "(2, 'amy', 20, 2020)", "(3, 'max', 10, 2022)",
               "(4, 'sue', 20, 2022)", "(5, 'bob', 30, 2020)", "(6, 'kim', 20, 2020)", "(7, 'art', 30, 2021)",
               "(8, 'pat', 20, 2019)", "(9, 'lee', 10, 2021)", "(10, 'alan', 30, 2022)", "(11, 'ben', 30, 2022)",
               "(12, 'cathy', 10, 2023)", "(13, 'don', 10, 2022)", "(14, 'eames', 30, 2021)",
               "(15, 'francis', 10, 2021)", "(16, 'gary', 30, 2022)", "(17, 'henry', 10, 2020)",
               "(18, 'Ganny', 10, 2020)", "(19, 'Lyn', 20, 2021)", "(20, 'Meredith', 30, 2022)",
               "(21, 'Tomasine', 40, 2020)", "(22, 'Evyn', 50, 2022)", "(23, 'Maible', 60, 2019)",
               "(24, 'Diarmid', 70, 2019)", "(25, 'Geneva', 80, 2022)", "(26, 'Greer', 90, 2020)",
               "(27, 'Cello', 100, 2022)", "(28, 'Levin', 110, 2022)", "(29, 'Courtenay', 120, 2022)",
               "(30, 'Colby', 130, 2022)", "(31, 'Harland', 140, 2019)", "(32, 'Athene', 150, 2019)",
               "(33, 'Augie', 160, 2022)", "(34, 'Isis', 170, 2020)", "(35, 'Billie', 180, 2021)",
               "(36, 'Maximilian', 190, 2022)", "(37, 'Tallou', 200, 2019)", "(38, 'Nan', 10, 2020)",
               "(39, 'Zulema', 20, 2020)", "(40, 'Annabella', 30, 2022)", "(41, 'Janeva', 40, 2020)",
               "(42, 'Laina', 50, 2021)", "(43, 'Laurena', 60, 2019)", "(44, 'Peyton', 70, 2019)",
               "(45, 'Willie', 80, 2021)", "(46, 'Oriana', 90, 2019)", "(47, 'Theodore', 100, 2020)",
               "(48, 'Lindie', 110, 2020)", "(49, 'Vance', 120, 2019)", "(50, 'Hedvig', 130, 2020)",
               "(51, 'Hamilton', 140, 2020)", "(52, 'Adria', 150, 2020)", "(53, 'Cayla', 160, 2022)",
               "(54, 'Felicio', 170, 2021)", "(55, 'Danica', 180, 2019)", "(56, 'Hilde', 190, 2020)",
               "(57, 'Conrad', 200, 2019)", "(58, 'Raoul', 10, 2021)", "(59, 'Cristobal', 20, 2022)",
               "(60, 'Ludwig', 30, 2019)" };
         for (int i = 0; i < studvals.length; i++)
            numRecordsUpdated = planner.executeUpdate(s + studvals[i], tx);
         System.out.println(numRecordsUpdated + " STUDENT records inserted.");

         s = "create table DEPT(DId int, DName varchar(8))";
         numRecordsUpdated = planner.executeUpdate(s, tx);
         System.out.println("Table DEPT created.");

         s = "insert into DEPT(DId, DName) values ";
         String[] deptvals = { "(10, 'compsci')", "(20, 'math')", "(30, 'drama')", "(40, 'Marketing')",
               "(50, 'Accounting')", "(60, 'Mechanical Engineering')", "(70, 'Environmental Engineering')",
               "(80, 'Human Resources')", "(90, 'Research and Development')", "(100, 'Services')", "(110, 'Arts')",
               "(120, 'Product Management')", "(130, 'Systems Engineering')", "(140, 'Finance')", "(150, 'Support')",
               "(160, 'Engineering Science')", "(170, 'Chemical Engineering')", "(180, 'Biomedical Engineering')",
               "(190, 'Software Engineering')", "(200, 'Sales')" };
         for (int i = 0; i < deptvals.length; i++)
            numRecordsUpdated = planner.executeUpdate(s + deptvals[i], tx);
         System.out.println(numRecordsUpdated + " DEPT records inserted.");

         s = "create table COURSE(CId int, Title varchar(20), DeptId int)";
         numRecordsUpdated = planner.executeUpdate(s, tx);
         System.out.println("Table COURSE created.");

         s = "insert into COURSE(CId, Title, DeptId) values ";
         String[] coursevals = { "(12, 'db systems', 10)", "(22, 'compilers', 10)", "(32, 'calculus', 20)",
               "(42, 'algebra', 20)", "(52, 'acting', 30)", "(62, 'elocution', 30)" };
         for (int i = 0; i < coursevals.length; i++)
            numRecordsUpdated = planner.executeUpdate(s + coursevals[i], tx);
         System.out.println(numRecordsUpdated + " COURSE records inserted.");

         s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
         numRecordsUpdated = planner.executeUpdate(s, tx);
         System.out.println("Table SECTION created.");

         s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
         String[] sectvals = { "(13, 12, 'turing', 2018)", "(23, 12, 'turing', 2019)", "(33, 32, 'newton', 2019)",
               "(43, 32, 'einstein', 2017)", "(53, 62, 'brando', 2018)" };
         for (int i = 0; i < sectvals.length; i++)
            numRecordsUpdated = planner.executeUpdate(s + sectvals[i], tx);
         System.out.println(numRecordsUpdated + " SECTION records inserted.");

         s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
         numRecordsUpdated = planner.executeUpdate(s, tx);
         System.out.println("Table ENROLL created.");

         s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
         String[] enrollvals = { "(14, 1, 13, 'A')", "(24, 1, 43, 'C' )", "(34, 2, 43, 'B+')", "(44, 4, 33, 'B' )",
               "(54, 4, 53, 'A' )", "(64, 6, 53, 'A' )" };
         for (int i = 0; i < enrollvals.length; i++)
            numRecordsUpdated = planner.executeUpdate(s + enrollvals[i], tx);
         System.out.println(numRecordsUpdated + " ENROLL records inserted.");

         tx.commit();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }
}
