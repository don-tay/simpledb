package test;

import java.sql.Types;
import java.util.List;
import java.util.Scanner;

import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class SimpleIJ {
	private static final String DEFAULT_DB_NAME = "studentdb";

	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
		// Default to studentdb for faster testing
		// System.out.print("Enter the name of DB to connect: ");
		// String dbName = (sc.hasNext()) ? sc.next().trim() : DEFAULT_DB_NAME;
		String dbName = DEFAULT_DB_NAME;

		try {
			// analogous to the driver
			SimpleDB db = new SimpleDB(dbName);

			// analogous to the connection
			Transaction tx = db.newTx();
			Planner planner = db.planner();

			System.out.println("Successfully connected to " + dbName);
			System.out.print("\nSQL> ");
			while (sc.hasNextLine()) {
				// process one line of input
				String cmd = sc.nextLine().trim();
				if (cmd.startsWith("exit"))
					break;
				else if (cmd.startsWith("select"))
					doQuery(planner, cmd, tx);
				else
					doUpdate(planner, cmd, tx);
				System.out.print("\nSQL> ");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		sc.close();
		System.exit(0);
	}

	private static void doQuery(Planner planner, String cmd, Transaction tx) {
		try {
			Plan p = planner.createQueryPlan(cmd, tx);
			List<String> fieldNames = p.schema().fields();
			int totalWidth = 0;

			// print header
			for (String fieldName : fieldNames) {
				int width = fieldName.length() + 1;
				totalWidth += width;
				String fmt = "%" + width + "s";
				System.out.format(fmt, fieldName);
			}
			System.out.println();
			for (int i = 0; i < totalWidth; i++)
				System.out.print("-");
			System.out.println();

			Scan s = p.open();
			// print records
			while (s.next()) {
				for (String fieldName : fieldNames) {
					int fldtype = p.schema().type(fieldName);
					String fmt = "%" + fieldName.length();
					if (fldtype == Types.INTEGER) {
						int ival = s.getInt(fieldName);
						System.out.format(fmt + "d", ival);
					} else {
						String sval = s.getString(fieldName);
						System.out.format(fmt + "s", sval);
					}
				}
				System.out.println();
			}
			tx.commit();
		} catch (Exception e) {
			System.out.println("SQL Exception: " + e.toString());
		}
	}

	private static void doUpdate(Planner planner, String cmd, Transaction tx) {
		try {
			int numRecordsUpdated = planner.executeUpdate(cmd, tx);
			tx.commit();
			System.out.println(numRecordsUpdated + " records processed");
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception: " + e.toString());
		}
	}
}
