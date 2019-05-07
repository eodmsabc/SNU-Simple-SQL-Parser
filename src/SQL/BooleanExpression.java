package SQL;

import java.util.ArrayList;

public class BooleanExpression {
	BooleanNode root;
	
	public BooleanExpression(BooleanNode node) {
		root = node;
	}
	
	public ArrayList<ArrayList<Value>> filter(Relation r) throws MyException {
		if (root == null) {
			return r.getRecords();
		}
		
		boolean check;
		ArrayList<Attribute> schema = r.getSchema();
		ArrayList<ArrayList<Value>> records = r.getRecords();
		int size = schema.size();
		
		ArrayList<ArrayList<Value>> result = new ArrayList<ArrayList<Value>>();
		
		try {
			for (int i = 0; i < size; i++) {
				check = root.evaluate(records.get(i), schema);
				if (check) {
					result.add(records.get(i));
				}
			}
		}
		catch (MyException e) {
			throw e;
		}
		return result;
	}
}
