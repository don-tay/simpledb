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
               "(21, 'Tomasine', 40, 2020)", "(22, 'Evyn', 50, 2022)", "(23,'Maible',60,2019)",
               "(24, 'Diarmid', 70, 2019)", "(25, 'Geneva', 80, 2022)", "(26,'Greer',90,2020)",
               "(27, 'Cello', 100, 2022)", "(28, 'Levin', 11, 2022)", "(29,'Courtenay',12, 2022)",
               "(30, 'Colby', 13, 2022)", "(31, 'Harland', 14, 2019)", "(32, 'Athene',15,2019)",
               "(33, 'Augie', 16, 2022)", "(34, 'Isis', 17, 2020)", "(35, 'Billie',18,2021)",
               "(36, 'Maximilian', 19, 2022)", "(37, 'Tallou', 20, 2019)", "(38, 'Nan',10,2020)",
               "(39, 'Zulema', 20, 2020)", "(40, 'Annabella', 30, 2022)", "(41, 'Janeva',40, 2020)",
               "(42, 'Laina', 50, 2021)", "(43, 'Laurena', 60, 2019)", "(44, 'Peyton',70,2019)",
               "(45, 'Willie', 80, 2021)", "(46, 'Oriana', 90, 2019)", "(47, 'Theodore',100,2020)",
               "(48, 'Lindie', 11, 2020)", "(49, 'Vance', 12, 2019)", "(50, 'Hedvig',13,2020)" };
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
               "(190, 'Software Engineering')", "(200, 'Sales')", "(210, 'RELAX NG')", "(220, 'XaaS')",
               "(230, 'dtSearch')", "(240, 'Ecological Restoration')", "(250, 'EEG')", "(260, 'DFR')",
               "(270, 'Solar PV')", "(280, 'Job Scheduling')", "(290, 'Hypermesh')", "(300, 'Digital Journalism')",
               "(310, 'Client Aquisition')", "(320, 'TD-SCDMA')", "(330, 'Digital Illustration')", "(340, 'WSS 2.0')",
               "(350, 'HCFA')", "(360, 'PFlow')", "(370, 'RFID')", "(380, 'Hardware Diagnostics')", "(390, 'RFP')",
               "(400, 'ESB')", "(410, 'Thermal Oxidation')", "(420, 'NBAR')", "(430, 'PWM')",
               "(440, 'Urban Economics')", "(450, 'Ethics')", "(460, 'jQuery')", "(470, 'IRI Xlerate')",
               "(480, 'MTTR')", "(490, 'UART')", "(500, 'XSI')" };
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
               "(372, 'Eaglesoft', 110)", "(382, 'Assessment Center', 120)", "(392,'Performing Arts', 130)",
               "(402, 'Reaction Kinetics', 140)", "(412, 'Utilization', 150)", "(422,'Navigation', 160)",
               "(432, 'CMTS', 170)", "(442, 'Overcome Objections', 180)", "(452, 'JointMilitary Operations', 190)",
               "(462, 'Igneous Petrology', 200)", "(472, 'Commercial Piloting', 10)", "(482,'OmniPlan', 20)",
               "(492, 'ICP-MS', 30)", "(502, 'Pyramix', 40)" };
         for (int i = 0; i < coursevals.length; i++)
            numRecordsUpdated = planner.executeUpdate(s + coursevals[i], tx);
         System.out.println(numRecordsUpdated + " COURSE records inserted.");

         s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
         numRecordsUpdated = planner.executeUpdate(s, tx);
         System.out.println("Table SECTION created.");

         s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
         String[] sectvals = { "(13, 12, 'turing', 2018)", "(23, 12, 'turing', 2019)", "(33, 32, 'newton', 2019)",
               "(43, 32, 'einstein', 2017)", "(53, 62, 'brando', 2018)", "(63, 12, 'Magdalene', 2017)",
               "(73, 22, 'Melina', 2017)", "(83, 32, 'Leone', 2016)", "(93, 42, 'Oceane', 2018)",
               "(103, 52, 'Oceanne', 2016)", "(113, 62, 'Valerie', 2017)", "(123, 72, 'Simplifies', 2022)",
               "(133, 82, 'Eugenie', 2019)", "(143, 92, 'Melanie', 2016)", "(153, 102, 'Lois', 2018)",
               "(163, 112, 'Reserves', 2022)", "(173, 122, 'Maelyss', 2021)", "(183, 132, 'Marie-noel', 2021)",
               "(193, 142, 'Gorel', 2018)", "(203, 152, 'Mylene', 2020)", "(213, 162, 'Dorothee', 2016)",
               "(223, 172, 'Maena', 2021)", "(233, 182, 'Gaia', 2022)", "(243, 192, 'Personnalisee', 2021)",
               "(253, 202, 'Melodie', 2017)", "(263, 212, 'Tan', 2019)", "(273, 222, 'Annotes', 2018)",
               "(283, 232, 'Gosta', 2022)", "(293, 242, 'Rebecca', 2017)", "(303, 252, 'Lie', 2021)",
               "(313, 262, 'Cecile', 2016)", "(323, 272, 'Mai', 2016)", "(333, 282,'Penelope', 2019)",
               "(343, 292, 'Rao', 2018)", "(353, 302, 'Personnalisee', 2017)", "(363, 312,'Fei', 2020)",
               "(373, 322, 'Kui', 2020)", "(383, 332, 'Zoe', 2018)", "(393, 342,'Personnalisee', 2017)",
               "(403, 352, 'Marie-francoise', 2018)", "(413, 362, 'Eloise', 2016)", "(423,372, 'Berengere', 2022)",
               "(433, 382, 'Gaelle', 2016)", "(443, 392, 'Maite', 2016)", "(453, 402, 'Uo',2018)",
               "(463, 412, 'Naelle', 2020)", "(473, 422, 'Laurena', 2022)", "(483, 432,'Maite', 2022)",
               "(493, 442, 'Kevina', 2022)", "(503, 452, 'Solene', 2022)" };
         for (int i = 0; i < sectvals.length; i++)
            numRecordsUpdated = planner.executeUpdate(s + sectvals[i], tx);
         System.out.println(numRecordsUpdated + " SECTION records inserted.");

         s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
         numRecordsUpdated = planner.executeUpdate(s, tx);
         System.out.println("Table ENROLL created.");

         s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
         String[] enrollvals = { "(14, 1, 13, 'A')", "(24, 1, 43, 'C' )", "(34, 2, 43, 'B+')", "(44, 4, 33, 'B' )",
               "(54, 4, 53, 'A' )", "(64, 6, 53, 'A' )", "(74, 1, 13, 'C+')", "(84, 2, 23, 'A-')", "(94, 3, 33, 'A+')",
               "(104, 4, 43, 'C+')", "(114, 5, 53, 'B+')", "(124, 6, 63, 'A-')", "(134, 7, 73, 'B-')",
               "(144, 8, 83, 'B')", "(154, 9, 93, 'B')", "(164, 10, 103, 'A+')", "(174, 11, 113, 'A')",
               "(184, 12, 123, 'C+')", "(194, 13, 133, 'C+')", "(204, 14, 143, 'A')", "(214, 15, 153, 'B-')",
               "(224, 16, 163, 'A+')", "(234, 17, 173, 'A')", "(244, 18, 183, 'A')", "(254, 19, 193, 'A+')",
               "(264, 20, 203, 'B+')", "(274, 21, 213, 'C+')", "(284, 22, 223, 'B')", "(294, 23, 233, 'B+')",
               "(304, 24, 243, 'B+')", "(314, 25, 253, 'B-')", "(324, 26, 263, 'B')", "(334, 27, 273, 'C+')",
               "(344, 28, 283, 'A-')", "(354, 29, 293, 'B')", "(364, 30, 303, 'C+')", "(374, 31, 313, 'A+')",
               "(384, 32, 323, 'B')", "(394, 33, 333, 'A+')", "(404, 34, 343, 'C+')", "(414, 35, 353, 'C+')",
               "(424, 36, 363, 'A-')", "(434, 37, 373, 'A-')", "(444, 38, 383, 'A-')", "(454, 39, 393, 'B')",
               "(464, 40, 403, 'A')", "(474, 41, 413, 'A-')", "(484, 42, 423, 'A+')", "(494, 43, 433, 'A-')",
               "(504, 44, 443, 'B+')" };
         for (int i = 0; i < enrollvals.length; i++)
            numRecordsUpdated = planner.executeUpdate(s + enrollvals[i], tx);
         System.out.println(numRecordsUpdated + " ENROLL records inserted.");

         tx.commit();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }
}
