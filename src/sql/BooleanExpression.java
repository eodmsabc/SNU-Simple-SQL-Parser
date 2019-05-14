package sql;

import java.util.ArrayList;

public class BooleanExpression {
	BooleanNode root;
	
	public BooleanExpression(BooleanNode node) {
		root = node;
	}
	
	// Return filtered records
	public ArrayList<ArrayList<Value>> filter(Relation r) throws MyException {
		if (root == null) {
			return r.getRecords();
		}

		int check;
		ArrayList<Attribute> schema = r.getSchema();
		ArrayList<ArrayList<Value>> records = r.getRecords();
		
		ArrayList<ArrayList<Value>> result = new ArrayList<ArrayList<Value>>();
		
		try {
			for (ArrayList<Value> rec : records) {
				check = root.evaluate(rec, schema);
				
				if (check == 1) {
					result.add(rec);
				}
			}
		}
		catch (MyException e) {
			throw e;
		}
		return result;
	}
	
	// For debugging
	@Override
	public String toString() {
		if (root == null) {
			return "true";
		}
		else {
			return root.toString();
		}
	}
}
