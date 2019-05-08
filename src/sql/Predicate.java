package sql;

import java.util.ArrayList;

public class Predicate {

	Value[] v;
	Comparator op;
	String[] t, c;
	
	private Predicate(Comparator comp) {
		op = comp;
		v = new Value[2];
		t = new String[2];
		c = new String[2];
	}
	public Predicate(Comparator comp, String t0, String c0) {
		this(comp);
		t[0] = t0;
		c[0] = c0;
	}
	
	public static Predicate generate(Comparator comp, ValueCompare vc0, ValueCompare vc1) {
		
		Predicate newPredicate = new Predicate(comp);
		
		if (vc0.isConstValue()) {
			newPredicate.v[0] = vc0.value;
		}
		else {
			newPredicate.t[0] = vc0.tableName;
			newPredicate.c[0] = vc0.columnName;
		}
		
		if (vc1.isConstValue()) {
			newPredicate.v[1] = vc1.value;
		}
		else {
			newPredicate.t[1] = vc1.tableName;
			newPredicate.c[1] = vc1.columnName;
		}
		
		return newPredicate;
	}
	
	private boolean isConst(int i) {
		return (v[i] != null);
	}
	
	public String tableCol(int i) {
		if (t[i] == null) return "." + c[i];
		else return t[i] + "."  + c[i];
	}
	
	public int evaluate(ArrayList<Value> rec, ArrayList<Attribute> schema) throws MyException {
		int retval = -1;
		Value[] cp = new Value[2];
		
		try {
			for (int i = 0; i < 2; i++) {
				if (i == 1 && (op == Comparator.IN || op == Comparator.INN)) break;
				if (isConst(i)) {
					cp[i] = v[i];
				}
				else {
					cp[i] = findValue(i, rec, schema);
				}
			}
			
			if (!Value.comparable(cp[0], cp[1])) {
				throw new MyException(new DBMessage(MsgType.WhereIncomparableError));
			}
		}
		catch (MyException e) {
			throw e;
		}
		
		if (op == Comparator.IN) {
			if (cp[0].isNull()) return 1;
			else return -1;
		}
		else if (op == Comparator.INN) {
			if (cp[0].isNull()) return -1;
			else return 1;
		}
		
		if (cp[0].isNull() || cp[1].isNull()) {
			return 0;
		}
		
		int diff = cp[0].compareTo(cp[1]);

		switch(op) {
			case EQ:
				if (diff == 0) retval = 1;
				break;
			case NEQ:
				if (diff != 0) retval = 1;
				break;
				
			case GTE:
				if (diff == 0) retval = 1;
			case GT:
				if (diff > 0) retval = 1;
				break;
				
			case LTE:
				if (diff == 0) retval = 1;
			default:
				if (diff < 0) retval = 1;
				break;
		}
		
		return retval;
	}
	
	private Value findValue(int i, ArrayList<Value> rec, ArrayList<Attribute> schema) throws MyException {
		if (t[i] == null && c[i] == null) {
			return new Value();
		}
		
		String col = tableCol(i);
		int size = schema.size();
		int index = -1;
		int tableIndex = 1;
		String colName;
		
		for(int idx = 0; idx < size; idx++) {
			colName = schema.get(idx).getName();
			if (Relation.lastMatch(colName, col) >= 0) {
				if (index < 0) {
					index = idx;
				}
				else {
					throw new MyException(new DBMessage(MsgType.WhereAmbiguousReference));
				}
			}
			else if (t[i] != null) {
				if (tableIndex == 1) tableIndex = -1;
				if(colName.substring(0, colName.indexOf('.')).equals(t[i])) {
					tableIndex = 0;
				}
			}
		}
		
		if (tableIndex < 0) {
			throw new MyException(new DBMessage(MsgType.WhereTableNotSpecified));
		}
		
		if (index < 0) {
			throw new MyException(new DBMessage(MsgType.WhereColumnNotExist));
		}
		
		return rec.get(index);
	}
}
