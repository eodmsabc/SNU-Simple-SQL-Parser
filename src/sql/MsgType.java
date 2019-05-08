package sql;

public enum MsgType {
	NotImplemented,
	SyntaxError,
	NoMessage,

	// Create Table
	CreateTableSuccess, // tableName (String) Required
	DuplicateColumnDefError,
	DuplicatePrimaryKeyDefError,
	ReferenceTypeError,
	ReferenceNonPrimaryKeyError,
	ReferenceColumnExistenceError,
	ReferenceTableExistenceError,
	NonExistingColumnDefError, // columnName (String)
																							// Required
	TableExistenceError,

	CharLengthError,

	// Drop Table
	DropSuccess, // tableName (String) Required
	DropReferencedTableError,
	NoSuchTable, // Desc

	// Show Tables
	ShowTablesNoTable,
	
	// Insert
	InsertResult,
	InsertDuplicatePrimaryKeyError,
	InsertReferentialIntegrityError,
	InsertTypeMismatchError,
	InsertColumnExistenceError,	// columnName (String)
	InsertColumnNonNullableError,	// columnName (String)
	
	// Delete
	DeleteResult, // deleted count (String or int)
	DeleteReferentialIntegrityPassed,	// not deleted due to referential integrity
	
	// Select
	SelectTableExistenceError,	// tableName (String)
	SelectColumnResolveError,	// columnName (String)
	
	// Where
	WhereIncomparableError,
	WhereTableNotSpecified,
	WhereColumnNotExist,
	WhereAmbiguousReference
}