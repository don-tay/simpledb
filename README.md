# SimpleDB

Extension of SimpleDB, a lightweight SQL RDBMS that supports indexes, multi-tenancy, and transactions.
Implemented as part of CS3223 Database Implementation project.

# Setup

## Installation

- OpenJDK 11 or higher

```
java --version # Java RE version should output >11
```

## Code Editor

Eclipse is the recommended code editor.

For VS Code, the following VSC extensions are recommended (VSC will prompt to install these extentions on launch):

- vscjava.vscode-java-pack

## Run Database

1. Ensure the `studentdb` directory is created. Otherwise, run `test/CreateStudentDB` script.
2. Run `test/SimpleIJ` script.
3. Database is running. Enter database commands in the command line.

> NOTE: Should you run into issues, delete the `studentdb` directory and repeat from step 1

# Acknowledgement

Implementation is an extension of SimpleDB from Edward Sciore. Project is used solely for educational purpose.

# Queries to run

## Create tables with 50 - 100 rows, some duplicate rows

Done in the CreateStudentDB2.java file

## Single Table Queries

### SELECT

`select sid, sname from student`

### WHERE (equality)

`select sid, sname from student where sid = 5`

### WHERE (non equality)

`select sid, sname from student where sname > 'bob'`

### Search using index

#### Btree Index

`select sid, majorid from student where sid = 10`

#### Hash Index

`select sid, majorid from student where majorid = 20`

### ORDER BY

#### Single ORDER BY

`select cid, title, deptid from course order by cid`

#### Multiple ORDER BY

`select gradyear, sname, sid from student order by gradyear desc, sid desc`

### Group By

#### Single field group by

`select yearoffered from section group by yearoffered`

#### Multiple field group by

`select yearoffered, courseid from section group by yearoffered, courseid`

### Aggregates

#### Single Aggregate

`select avg(eid) from enroll`

#### Multiple Aggregates

`select avg(eid), min(studentid), sum(sectionid) from enroll`

#### Group by and aggregate

`select studentid, count(eid) from enroll group by studentid`
`select prof, courseid, min(yearoffered), max(yearoffered) from section group by prof, courseid`

### Distinct

`select distinct yearoffered from section`
`select distinct majorid, gradyear from student`

### Multiple conditions

`select gradyear, count(sid) from student where gradyear > 2020 and sname > 'bob' group by gradyear order by countofsid`

## Two table joins

`select sid,studentid,grade from student,enroll where sid>studentid`
`select sid,studentid,sname, grade from enroll, student where sid=studentid`

### Index Join

`select sid,majorid,did from dept,student where majorid=did`

## Four table joins

Hash/Sort-merge/Nested-loop Join:
`select sid,sname,dname,title,grade from student,dept,course,enroll where sid=studentid and deptid=did and majorid=did`

Idx Join:
`select sid,sname,did,title,grade from dept,course,enroll,student where sid=studentid and majorid=did and deptid=did`

## Additional queries

### Behaviour for DISTINCT or GROUP BY used together with ORDER BY

Following Postgres, there are 2 cases for ORDER BY:

- If there is GROUP BY or DISTINCT then the sort fields must be found in the GROUP BY or DISTINCT field.

  1. Positive case:
     `select distinct sname from student order by sname`
     `select sname from student group by sname order by sname`
  2. Error cases:
     `select distinct sname from student order by sid`
     `select sname from student group by sname order by sid`

- If there is no GROUP BY or DISTINCT then can sort on any field in the tables (beyond what is being projected).
  `select sname from student order by sid`
