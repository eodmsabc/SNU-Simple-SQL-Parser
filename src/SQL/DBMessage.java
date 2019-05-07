package SQL;

public class DBMessage {
	public DBMessage() {
		this(MsgType.NoMessage, null);
	}

	public DBMessage(MsgType t) {
		this(t, null);
	}

	public DBMessage(MsgType t, String id) {
		type = t;
		identifier = id;
	}

	public DBMessage(MsgType t, int cnt) {
		type = t;
		count = cnt;
	}

	public MsgType type;
	public String identifier;
	public int count;

	@Override
	public String toString() {
		switch (type) {
		case SyntaxError:
			return "Syntax error";

		// Create Table
		case CreateTableSuccess:
			return "\'" + identifier + "\' table is created";
		case DuplicateColumnDefError:
			return "Create table has failed: column definition is duplicated";
		case DuplicatePrimaryKeyDefError:
			return "Create table has failed: primary key definition is duplicated";
		case ReferenceTypeError:
			return "Create table has failed: foreign key references wrong type";
		case ReferenceNonPrimaryKeyError:
			return "Create table has failed: foreign key references non primary key column";
		case ReferenceColumnExistenceError:
			return "Create table has failed: foreign key references non existing column";
		case ReferenceTableExistenceError:
			return "Create table has failed: foreign key references non existing table";
		case NonExistingColumnDefError:
			return "Create table has failed: \'" + identifier + "\' does not exists in column definition";
		case TableExistenceError:
			return "Create table has failed: table with the same name already exists";
		case CharLengthError:
			return "Char length should be over 0";

		// Drop Table
		case DropSuccess:
			return "\'" + identifier + "\' table is dropped";
		case DropReferencedTableError:
			return "Drop table has failed: \'" + identifier + "\' is referenced by other table";
		case NoSuchTable:
			return "No such table";

		// Show Tables
		case ShowTablesNoTable:
			return "There is no table";

		// Insert
		case InsertResult:
			return "The row is inserted";
		case InsertDuplicatePrimaryKeyError:
			return "Insertion has failed: Primary key duplication";
		case InsertReferentialIntegrityError:
			return "Insertion has failed: Referential integrity violation";
		case InsertTypeMismatchError:
			return "Insertion has failed: Types are not matched";
		case InsertColumnExistenceError:
			return "Insertion has failed: '" + identifier + "' does not exist";
		case InsertColumnNonNullableError:
			return "Insertion has failed: '" + identifier + "' is not nullable";

		// Delete
		case DeleteResult:
			return count + " row(s) are deleted";
		case DeleteReferentialIntegrityPassed:
			return count + " row(s) are not deleted due to referential integrity";

		// Select
		case SelectTableExistenceError:
			return "Selection has failed: '" + identifier + "' does not exist";
		case SelectColumnResolveError:
			return "Selection has failed: fail to resolve '" + identifier + "'";
			
		// Where
		case WhereIncomparableError:
			return "Where clause try to compare incomparable values";
		case WhereTableNotSpecified:
			return "Where clause try to reference tables which are not specified";
		case WhereColumnNotExist:
			return "Where clause try to reference non existing column";
		case WhereAmbiguousReference:
			return "Where clause contains ambiguous reference";

		default:
			return "Not Implemented";
			 
		}
	}
}
