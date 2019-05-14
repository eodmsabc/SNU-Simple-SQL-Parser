package sql;

import java.util.ArrayList;

public class BooleanNode {
	BExprType type;
	BooleanNode b1, b2;
	Predicate predicate;
	
	public BooleanNode(char type, BooleanNode b1, BooleanNode b2) {
		this.type = BExprType.convert(type);
		this.b1 = b1;
		this.b2 = b2;
	}
	public BooleanNode(char type, BooleanNode b) {
		this.type = BExprType.convert(type);
		this.b1 = b;
	}
	public BooleanNode(Predicate p) {
		type = BExprType.B_PREDICATE;
		predicate = p;
	}
	
	// Evaluate for one record entity 'rec'
	public int evaluate(ArrayList<Value> rec, ArrayList<Attribute> schema) throws MyException {
		int retval;
		try {
			switch(type) {
				case B_NOT:
					retval = MyCalc.not(b1.evaluate(rec, schema));
					break;
				case B_AND:
					retval = b1.evaluate(rec, schema);
					if (retval != -1)	// Short Circuit Evaluation
					{
						retval = MyCalc.and(retval, b2.evaluate(rec, schema));
					}
					break;
				case B_OR:
					retval = b1.evaluate(rec, schema);
					if (retval != 1)	// Short Circuit Evaluation
					{
						retval = MyCalc.or(retval, b2.evaluate(rec, schema));
					}
					break;
				default:
					retval = predicate.evaluate(rec, schema);
					break;
			}
		}
		catch (MyException e) {
			throw e;
		}
		
		return retval;
	}
	
	// For debugging
	@Override
	public String toString() {
		switch(type) {
		case B_NOT:
			return "(!" + b1.toString() + ")";
		case B_AND:
			return "(" + b1.toString() + "&&" + b2.toString() + ")";
		case B_OR:
			return "(" + b1.toString() + "||" + b2.toString() + ")";
		default:
			return predicate.toString();
		}
	}
}

enum BExprType {
	B_NOT,
	B_AND,
	B_OR,
	B_PREDICATE;
	
	static BExprType convert(char str) {
		switch(str) {
			case '~': return B_NOT;
			case '&': return B_AND;
			case '|': return B_OR;
			default: return B_PREDICATE;
		}
	}
}