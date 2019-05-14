package sql;

import java.util.ArrayList;

public class Predicate {

	Value[] v;
	Comparator op;
	String[] t, c;
	private DataType[] type;
	
	private Predicate(Comparator comp) {
		op = comp;
		v = new Value[2];
		t = new String[2];
		c = new String[2];
		type = new DataType[2];
	}
	public Predicate(Comparator comp, String t0, String c0) {
		this(comp);
		t[0] = t0;
		c[0] = c0;
	}
	
	public Predicate(ColValTuple insert) {
		this(Comparator.EQ);
		v[0] = insert.value;
		c[1] = insert.columnName;
	}
	
	public static Predicate generate(Comparator comp, ColValTuple cv0, ColValTuple cv1) {
		
		Predicate newPredicate = new Predicate(comp);
		
		if (cv0.isConstValue()) {
			newPredicate.v[0] = cv0.value;
		}
		else {
			newPredicate.t[0] = cv0.tableName;
			newPredicate.c[0] = cv0.columnName;
		}
		
		if (cv1.isConstValue()) {
			newPredicate.v[1] = cv1.value;
		}
		else {
			newPredicate.t[1] = cv1.tableName;
			newPredicate.c[1] = cv1.columnName;
		}
		
		return newPredicate;
	}
	
	private boolean isConst(int i) {
		return (v[i] != null);
	}
	
	public int evaluate(ArrayList<Value> rec, ArrayList<Attribute> schema) throws MyException {
		int retval = -1;
		Value[] cp = new Value[2];
		
		try {
			for (int i = 0; i < 2; i++) {
				if (i == 1 && (op == Comparator.IN || op == Comparator.INN)) {
					if (op == Comparator.IN) {
						if (cp[0].isNull()) return 1;
						else return -1;
					}
					else if (op == Comparator.INN) {
						if (cp[0].isNull()) return -1;
						else return 1;
					}
				}
				if (isConst(i)) {
					cp[i] = v[i];
					type[i] = v[i].type;
				}
				else {
					cp[i] = findValue(i, rec, schema);
				}
			}
			
			if (type[0] != type[1]) {
				throw new MyException(MsgType.WhereIncomparableError);
			}
			
			if (!Value.comparable(cp[0], cp[1])) {
				throw new MyException(MsgType.WhereIncomparableError);
			}
		}
		catch (MyException e) {
			throw e;
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
	
	// Get value from rec which specified from tableName t[i] and columnName c[i]
	private Value findValue(int i, ArrayList<Value> rec, ArrayList<Attribute> schema) throws MyException {
		if (t[i] == null && c[i] == null) {
			System.out.println("Fuck");
			return new Value();
		}
		
		String col = makeFullName(i);
		int size = schema.size();
		int index = -1;
		boolean tableSpecified = t[i]==null? true : false;
		
		for(int idx = 0; idx < size; idx++) {
			Attribute attr = schema.get(idx);
			boolean match = attr.nameMatch(col);
			if (match) {
				tableSpecified = true;
				if (index < 0) {
					index = idx;
				}
				else {
					throw new MyException(MsgType.WhereAmbiguousReference);
				}
			}
			else if (t[i] != null) {
				if(attr.getTableName().equals(t[i])) {
					tableSpecified = true;
				}
			}
		}
		if (!tableSpecified) {
			throw new MyException(MsgType.WhereTableNotSpecified);
		}
		
		if (index < 0) {
			throw new MyException(MsgType.WhereColumnNotExist);
		}
		
		type[i] = schema.get(index).getDataType();
		return rec.get(index);
	}
	
	private String makeFullName(int i) {
		if (t[i] == null) {
			return c[i];
		}
		else {
			return t[i] + "." + c[i];
		}
	}
	
	// For debugging
	private String operandToString(int i) {
		if (v[i] == null) {
			return makeFullName(i);
		}
		else {
			if (v[i].type == DataType.TYPE_CHAR) {
				return "'" + v[i] + "'";
			}
			else {
				return v[i].toString();
			}
		}
	}
	
	// For debugging
	@Override
	public String toString() {
		switch(op) {
		case IN:
			return "(" + makeFullName(0) + " is null)";
		case INN:
			return "(" + makeFullName(0) + " is not null)";
		case EQ:
			return "(" + operandToString(0) + "=" + operandToString(1) + ")";
		case NEQ:
			return "(" + operandToString(0) + "!=" + operandToString(1) + ")";
		case LT:
			return "(" + operandToString(0) + "<" + operandToString(1) + ")";
		case LTE:
			return "(" + operandToString(0) + "<=" + operandToString(1) + ")";
		case GT:
			return "(" + operandToString(0) + ">" + operandToString(1) + ")";
		default:
			return "(" + operandToString(0) + ">=" + operandToString(1) + ")";
		}
	}
}
