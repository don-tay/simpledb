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
               "(15, 'francis', 10, 2021)", "(16, 'gary', 30, 2022)", "(17, 'henry', 10,2020)",
               "(18, 'Ganny', 10, 2020)", "(19, 'Lyn', 20, 2021)", "(20, 'Meredith', 30,2022)",
               "(21, 'Tomasine', 40, 2020)", "(22, 'Evyn', 50, 2022)", "(23, 'Maible', 60,2019)",
               "(24, 'Diarmid', 70, 2019)", "(25, 'Geneva', 80, 2022)", "(26, 'Greer', 90,2020)",
               "(27, 'Cello', 100, 2022)", "(28, 'Levin', 110, 2022)", "(29, 'Courtenay',120, 2022)",
               "(30, 'Colby', 130, 2022)", "(31, 'Harland', 140, 2019)", "(32, 'Athene',150, 2019)",
               "(33, 'Augie', 160, 2022)", "(34, 'Isis', 170, 2020)", "(35, 'Billie', 180,2021)",
               "(36, 'Maximilian', 190, 2022)", "(37, 'Tallou', 200, 2019)", "(38, 'Nan',10, 2020)",
               "(39, 'Zulema', 20, 2020)", "(40, 'Annabella', 30, 2022)", "(41, 'Janeva',40, 2020)",
               "(42, 'Laina', 50, 2021)", "(43, 'Laurena', 60, 2019)", "(44, 'Peyton', 70,2019)",
               "(45, 'Willie', 80, 2021)", "(46, 'Oriana', 90, 2019)", "(47, 'Theodore',100, 2020)",
               "(48, 'Lindie', 110, 2020)", "(49, 'Vance', 120, 2019)", "(50, 'Hedvig', 130,2020)" };
         for (int i = 0; i < studvals.length; i++)
            numRecordsUpdated = planner.executeUpdate(s + studvals[i], tx);
         System.out.println(numRecordsUpdated + " STUDENT records inserted.");

         s = "create table DEPT(DId int, DName varchar(16))";
         numRecordsUpdated = planner.executeUpdate(s, tx);
         System.out.println("Table DEPT created.");

         s = "insert into DEPT(DId, DName) values ";
         String[] deptvals = { "(10, 'compsci')", "(20, 'math')", "(30, 'drama')", "(40, 'Marketing')",
               "(50, 'Accounting')", "(60, 'Mechanical Engineering')", "(70, 'Environmental Engineering')",
               "(80, 'Human Resources')", "(90, 'Research and Development')", "(100,'Services')", "(110, 'Arts')",
               "(120, 'Product Management')", "(130, 'Systems Engineering')", "(140,'Finance')", "(150, 'Support')",
               "(160, 'Engineering Science')", "(170, 'Chemical Engineering')", "(180,'Biomedical Engineering')",
               "(190, 'Software Engineering')", "(200, 'Sales')" };
         for (int i = 0; i < deptvals.length; i++)
            numRecordsUpdated = planner.executeUpdate(s + deptvals[i], tx);
         System.out.println(numRecordsUpdated + " DEPT records inserted.");

         s = "create table COURSE(CId int, Title varchar(20), DeptId int)";
         numRecordsUpdated = planner.executeUpdate(s, tx);
         System.out.println("Table COURSE created.");

         s = "insert into COURSE(CId, Title, DeptId) values ";
         String[] coursevals = { "(12, 'db systems', 10)", "(22, 'compilers', 10)", "(32, 'calculus', 20)",
               "(42, 'algebra', 20)", "(52, 'acting', 30)", "(62, 'elocution', 30)", "(72, 'Security Operations', 10)",
               "(82, 'Fluid Mechanics', 20)", "(92, 'IVIG', 30)", "(102, 'Ulead VideoStudio', 40)", "(112, 'Zen', 50)",
               "(122, 'Quality Patient Care', 60)", "(132, 'Vessel Operations', 70)", "(142, 'Academic Advising', 80)",
               "(152, 'PeopleSoft', 90)", "(162, 'Space Planning', 100)", "(172, 'SDL Tridion', 110)",
               "(182, 'Data Integration', 120)", "(192, 'DMAIC', 130)", "(202, 'Import', 140)", "(212, 'SSPS', 150)",
               "(222, 'HMDA', 160)", "(232, 'HSIA', 170)", "(242, 'CPIM', 180)", "(252, 'HR Transformation', 190)",
               "(262, 'Amazon EBS', 200)", "(272, 'MCH', 10)", "(282, 'BtB', 20)", "(292, 'PPE', 30)",
               "(302, 'NT 4.0', 40)", "(312, 'EOR', 50)", "(322, 'ATLS', 60)", "(332, 'Sports Marketing', 70)",
               "(342, 'Artistic Abilities', 80)", "(352, 'Benefits Administration', 90)", "(362, 'XForms', 100)",
               "(372, 'Eaglesoft', 110)", "(382, 'Assessment Center', 120)", "(392, 'Performing Arts', 130)",
               "(402, 'Reaction Kinetics', 140)", "(412, 'Utilization', 150)", "(422, 'Navigation', 160)",
               "(432, 'CMTS', 170)", "(442, 'Overcome Objections', 180)", "(452, 'Joint Military Operations', 190)",
               "(462, 'Igneous Petrology', 200)", "(472, 'Commercial Piloting', 10)", "(482, 'OmniPlan', 20)",
               "(492, 'ICP-MS', 30)", "(502, 'Pyramix', 40)" };
         for (int i = 0; i < coursevals.length; i++)
            numRecordsUpdated = planner.executeUpdate(s + coursevals[i], tx);
         System.out.println(numRecordsUpdated + " COURSE records inserted.");

         s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
         numRecordsUpdated = planner.executeUpdate(s, tx);
         System.out.println("Table SECTION created.");

         s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
         String[] sectvals = { "(13, 12, 'turing', 2018)", "(23, 12, 'turing', 2019)", "(33, 32, 'newton', 2019)",
               "(43, 32, 'einstein', 2017)", "(53, 62, 'brando', 2018)", "(63, 12, 'Magdalène', 2017)",
               "(73, 22, 'Mélina', 2017)", "(83, 32, 'Léone', 2016)", "(93, 42, 'Océane', 2018)",
               "(103, 52, 'Océanne', 2016)", "(113, 62, 'Valérie', 2017)", "(123, 72, 'Simplifiés', 2022)",
               "(133, 82, 'Eugénie', 2019)", "(143, 92, 'Mélanie', 2016)", "(153, 102, 'Loïs', 2018)",
               "(163, 112, 'Réservés', 2022)", "(173, 122, 'Maëlyss', 2021)", "(183, 132, 'Marie-noël', 2021)",
               "(193, 142, 'Görel', 2018)", "(203, 152, 'Mylène', 2020)", "(213, 162, 'Dorothée', 2016)",
               "(223, 172, 'Maéna', 2021)", "(233, 182, 'Gaïa', 2022)", "(243, 192, 'Personnalisée', 2021)",
               "(253, 202, 'Mélodie', 2017)", "(263, 212, 'Tán', 2019)", "(273, 222, 'Annotés', 2018)",
               "(283, 232, 'Gösta', 2022)", "(293, 242, 'Rébecca', 2017)", "(303, 252, 'Liè', 2021)",
               "(313, 262, 'Cécile', 2016)", "(323, 272, 'Maï', 2016)", "(333, 282, 'Pénélope', 2019)",
               "(343, 292, 'Ráo', 2018)", "(353, 302, 'Personnalisée', 2017)", "(363, 312, 'Fèi', 2020)",
               "(373, 322, 'Kuí', 2020)", "(383, 332, 'Zoé', 2018)", "(393, 342, 'Personnalisée', 2017)",
               "(403, 352, 'Marie-françoise', 2018)", "(413, 362, 'Eloïse', 2016)", "(423, 372, 'Bérengère', 2022)",
               "(433, 382, 'Gaëlle', 2016)", "(443, 392, 'Maïté', 2016)", "(453, 402, 'Uò', 2018)",
               "(463, 412, 'Naëlle', 2020)", "(473, 422, 'Lauréna', 2022)", "(483, 432, 'Maïté', 2022)",
               "(493, 442, 'Kévina', 2022)", "(503, 452, 'Solène', 2022)" };
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
