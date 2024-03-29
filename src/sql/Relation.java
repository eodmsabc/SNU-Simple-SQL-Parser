package sql;

import java.io.Serializable;
import java.util.ArrayList;
import com.sleepycat.je.Database;

public class Relation implements Serializable {
	private static final long serialVersionUID = 1L;
	static final String EMPTY_RELATION = "--empty";
	static final String RESULT_RELATION = "--result";

	private String tableName;
	private ArrayList<Attribute> schema;
	private ArrayList<String> referredTableList;
	private ArrayList<ArrayList<Value>> records;
	private ArrayList<String> pKeys;
	private ArrayList<ForeignKeyConstraint> fKeys;

	// Constructor
	public Relation(String tableName) {
		this.tableName = tableName;
		schema = new ArrayList<Attribute>();
		referredTableList = new ArrayList<String>();
		records = new ArrayList<ArrayList<Value>>();
	}

	// Getter, Setter and some trivial methods
	public String getTableName() {
		return tableName;
	}
	
	boolean isEmptyRelation() {
		return tableName.equals(EMPTY_RELATION);
	}

	public ArrayList<Attribute> getSchema() {
		return schema;
	}

	public ArrayList<String> getreferredTableList() {
		return referredTableList;
	}

	public ArrayList<ArrayList<Value>> getRecords() {
		return records;
	}

	public ArrayList<String> getPrimaryKeys() {
		return pKeys;
	}

	public ArrayList<ForeignKeyConstraint> getForeignKeyConstraint() {
		return fKeys;
	}

	public Attribute getAttribute(String colName) {
		if (colName.indexOf('.') < 0) {	// only column name
			for (Attribute attr : schema) {
				if (attr.getName().equals(colName)) {
					return attr;
				}
			}
			return null;
		}
		else {	// with table name
			for (Attribute attr : schema) {
				if (attr.getFullName().equals(colName)) {
					return attr;
				}
			}
			return null;
		}
	}

	public ArrayList<String> getColumnList() {
		ArrayList<String> colList = new ArrayList<String>();
		for (Attribute attr : schema) {
			colList.add(attr.getName());
		}
		return colList;
	}

	public ArrayList<String> getTableReferingColumns(String refTable) {
		ArrayList<String> refColList = new ArrayList<String>();

		for (Attribute attr : schema) {
			if (refTable.equals(attr.getRefTable())) {
				refColList.add(attr.getName());
			}
		}

		return refColList;
	}

	public int getIndexByColumnName(String col) {
		int size = schema.size();
		int ret = -1;
		for (int idx = 0; idx < size; idx++) {
			Attribute attr = schema.get(idx);
			if (attr.nameMatch(col)) {
				if (ret == -1) {
					ret = idx;
				}
				else {
					return -2;
				}
			}
		}
		return ret;
	}

	/*
	 * Creates Relation Schema
	 * colDefs: column definitions
	 * primary: primary key constraints (it can be more than one but make error here
	 * fKeyConstraints: foreign key constraints
	 */
	public DBMessage createSchema(Database db, ArrayList<Attribute> colDefs, ArrayList<ArrayList<String>> primary, ArrayList<ForeignKeyConstraint> fKeyConstraints) {
		DBMessage msg;
		
		if (primary.size() > 1) {
			return new DBMessage(MsgType.DuplicatePrimaryKeyDefError);
		}

		// Column Definition
		for (Attribute attr : colDefs) {
			// Existence Check
			if (getAttribute(attr.getName()) != null) {
				return new DBMessage(MsgType.DuplicateColumnDefError);
			}
			
			if (attr.getDataType() == DataType.TYPE_CHAR) {
				// Check Negative Char Length
				if (attr.getCharLength() <= 0) {
					return new DBMessage(MsgType.CharLengthError);
				}
			}
			attr.setFullName(tableName);
			addAttribute(attr);
		}
		
		// Primary Key Constraints
		if (primary.size() == 1) {
			for (String col : primary.get(0)) {
				boolean colFound = setPrimary(col);
				if (colFound == false) {
					return new DBMessage(MsgType.NonExistingColumnDefError, col);
				}
			}
			pKeys = primary.get(0);
		} else {
			pKeys = new ArrayList<String>();
		}
		
		// Foreign Key Constraints
		Relation foreignRelation;
		for (ForeignKeyConstraint elem : fKeyConstraints) {
			foreignRelation = db_search(db, elem.refTable);

			if (foreignRelation == null) {
				return new DBMessage(MsgType.ReferenceTableExistenceError);
			}

			// Check the count of reference column
			int forKeySize = elem.foreignKeys.size();
			int refKeySize = elem.referingKeys.size();
			if (forKeySize != refKeySize) {
				return new DBMessage(MsgType.ReferenceTypeError);
			}

			msg = referenceCheck(foreignRelation, elem);

			if (msg != null) {
				return msg;
			}
		}
		fKeys = fKeyConstraints;
		
		// Modify other relation's referenceList & nullable set
		for (ForeignKeyConstraint fkc : fKeys) {
			boolean nullable = true;
			for (String fcol : fkc.foreignKeys) {
				nullable = nullable && getAttribute(fcol).isNullable();
			}
			fkc.nullable = nullable;
			addReferenceList(db, fkc.refTable);
		}

		return null;
	}

	// Check foreign key constraints for one foreign table
	public DBMessage referenceCheck(Relation foreign, ForeignKeyConstraint elem) {
		int keySize = elem.foreignKeys.size();

		for (int index = 0; index < keySize; index++) {
			String curCol = elem.foreignKeys.get(index);
			String refCol = elem.referingKeys.get(index);

			Attribute curAttr = getAttribute(curCol);
			if (curAttr == null) {
				return new DBMessage(MsgType.NonExistingColumnDefError, curCol);
			}

			Attribute refAttr = foreign.getAttribute(refCol);
			if (refAttr == null) {
				return new DBMessage(MsgType.ReferenceColumnExistenceError);
			}

			if (!refAttr.isPrimary()) {
				return new DBMessage(MsgType.ReferenceNonPrimaryKeyError);
			}

			if (!Attribute.checkTypeMatch(curAttr, refAttr)) {
				return new DBMessage(MsgType.ReferenceTypeError);
			}

			setForeign(curCol, foreign.getTableName(), refCol);
		}

		// Check if FK references all primary key set
		ArrayList<Attribute> refSchema = foreign.getSchema();

		for (Attribute attr : refSchema) {
			if (attr.isPrimary()) {
				if (!elem.referingKeys.contains(attr.getName())) {
					return new DBMessage(MsgType.ReferenceNonPrimaryKeyError);
				}
			}
		}

		return null;
	}

	private void addAttribute(Attribute attr) {
		schema.add(attr);
	}

	private boolean setPrimary(String colName) {
		Attribute attr = getAttribute(colName);
		if (attr == null) {
			return false;
		}
		
		attr.setPrimary();
		return true;
	}

	boolean setForeign(String colName, String refTable, String refCol) {
		Attribute attr = getAttribute(colName);
		if (attr == null) {
			return false;
		}

		attr.setForeign(refTable, refCol);
		return true;
	}

	String getRefTableOfKey(String foreignKey) {
		Attribute attr = getAttribute(foreignKey);
		if (attr == null) {
			return null;
		}
		else {
			return attr.getRefTable();
		}
	}

	public ArrayList<String> getReferingTableList() {
		ArrayList<String> referingTableList = new ArrayList<String>();
		for (ForeignKeyConstraint fkc : fKeys) {

			String refTableName = fkc.refTable;
			if (!referingTableList.contains(refTableName)) {
				referingTableList.add(refTableName);
			}
		}
		return referingTableList;
	}

	private void addReferenceList(Database db, String target) {
		Relation targetRel = Relation.db_search(db, target);
		if (targetRel == null)
			return;
		targetRel.addreferredTableList(tableName);
		Relation.db_replace(db, targetRel);
	}

	private void delReferenceList(Database db, String target) {
		Relation targetRel = Relation.db_search(db, target);
		if (targetRel == null)
			return;
		targetRel.delreferredTableList(tableName);
		Relation.db_replace(db, targetRel);
	}

	void addreferredTableList(String rTable) {
		if (referredTableList.contains(rTable))
			return;
		referredTableList.add(rTable);
	}

	void delreferredTableList(String rTable) {
		if (!referredTableList.contains(rTable))
			return;
		referredTableList.remove(rTable);
	}

	// Get Reference Count (this method is used for drop table)
	int getRefCount() {
		return referredTableList.size();
	}

	// Remove table name from referring tables
	public DBMessage dropCleanUp(Database db) {
		if (getRefCount() > 0) {
			return new DBMessage(MsgType.DropReferencedTableError, tableName);
		}

		ArrayList<String> refTableList = getReferingTableList();
		for (String refTable : refTableList) {
			delReferenceList(db, refTable);
		}
		return null;
	}

	// desc query output
	public String describe() {
		String desc = "table_name [" + tableName + "]\n";
		desc += "-------------------------------------------------\n";
		desc += String.format(Attribute.SCHEMA_FORMAT + "\n", "column_name", "type", "null", "key");
		for (Attribute attr : schema) {
			desc += attr.toString() + "\n";
		}
		desc += "-------------------------------------------------\n";

		return desc;
	}

	// parse insert query
	private ArrayList<ColValTuple> parseInputValue(ArrayList<String> colList, ArrayList<Value> valList) throws MyException {
		DBMessage msg;
		int schemaSize = schema.size();
		ArrayList<ColValTuple> retList = new ArrayList<ColValTuple>();
		for (int i = 0; i < schemaSize; i++) {
			retList.add(new ColValTuple(tableName, schema.get(i).getName()));
		}
		
		int colSize = colList.size();
		int valSize = valList.size();
		if (colSize == 0) {
			if (valSize != schemaSize) {
				throw new MyException(MsgType.InsertTypeMismatchError);
			}
			else {
				for (int i = 0; i < schemaSize; i++) {
					// Type Check
					msg = schema.get(i).typeCheck(valList.get(i));
					if (msg != null) {
						throw new MyException(msg);
					}
					retList.get(i).value = valList.get(i);
				}
			}
		}
		else {
			if (colSize != valSize) {
				throw new MyException(MsgType.InsertTypeMismatchError);
			}
			else {
				// Duplicate column name detection / not in project specification
				for (int i = 0; i < colList.size(); i++) {
					for (int j = i + 1; j < colList.size(); j++) {
						if (colList.get(i).equals(colList.get(j))) {
							throw new MyException(MsgType.InsertTypeMismatchError);
						}
					}
				}
				
				// Column Existence Check
				ArrayList<String> columnCheck = getColumnList();
				for (String c : colList) {
					if (!columnCheck.contains(c)) {
						throw new MyException(MsgType.InsertColumnExistenceError, c);
					}
				}
				
				for (int i = 0; i < schemaSize; i++) {
					int cListIdx = colList.indexOf(columnCheck.get(i));
					if (cListIdx < 0) {	// Null check
						if (!schema.get(i).isNullable()) {
							throw new MyException(MsgType.InsertColumnNonNullableError, schema.get(i).getName());
						}
						retList.get(i).value = new Value();
					}
					else {	// Type Check
						msg = schema.get(i).typeCheck(valList.get(cListIdx));
						if (msg != null) {
							throw new MyException(msg);
						}
						retList.get(i).value = valList.get(cListIdx);
					}
				}
			}
		}
		
		return retList;
	}

	// Check constraints for insert value tuple
	private DBMessage insertConstraintCheck(Database db, ArrayList<ColValTuple> cvTuple) {
		// Primary Key Constraint
		if (pKeys.size() > 0) {
			BooleanExpression pKeyCheck = generatePrimaryCheckExpr(cvTuple);
			ArrayList<ArrayList<Value>> searchResult;
			
			try {
				searchResult = pKeyCheck.filter(this);
			}
			catch (MyException e) {
				return e.getDBMessage();
			}
			
			if (searchResult.size() > 0) {
				return new DBMessage(MsgType.InsertDuplicatePrimaryKeyError);
			}
		}
		
		// Referential Integrity Check
		boolean refIntegrityCheckNeeded = true;
		for (ColValTuple cv : cvTuple) {
			if (cv.value.isNull()) {
				refIntegrityCheckNeeded = false;
				break;
			}
		}
		
		if (refIntegrityCheckNeeded) {
			for (ForeignKeyConstraint fkc : fKeys) {
				ArrayList<ColValTuple> fcv = ColValTuple.columnFilter(fkc.foreignKeys, cvTuple);
				for (int i = 0; i < fcv.size(); i++) {
					fcv.get(i).columnName = fkc.referingKeys.get(i);
				}
				
				Relation refr = Relation.db_search(db, fkc.refTable);
				BooleanExpression refbool = refr.generatePrimaryCheckExpr(fcv);
				
				ArrayList<ArrayList<Value>> fsearchList = refr.search(refbool);
				
				if (fsearchList.size() == 0) {
					return new DBMessage(MsgType.InsertReferentialIntegrityError);
				}
			}
		}
		
		return null;
	}

	// Insert records
	public DBMessage insertRecord(Database db, ArrayList<String> colList, ArrayList<Value> valList) {
		DBMessage msg;
		ArrayList<ColValTuple> cvTuple;
		
		try {
			cvTuple = parseInputValue(colList, valList);
		}
		catch (MyException e) {
			return e.getDBMessage();
		}
		
		msg = insertConstraintCheck(db, cvTuple);
		if (msg != null) {
			return msg;
		}
		
		ArrayList<Value> newRecord = new ArrayList<Value>();
		int size = cvTuple.size();

		for (int i = 0; i < size; i++) {
			newRecord.add(cvTuple.get(i).value);
		}
		records.add(newRecord);
		
		return null;
	}

	// Create BooleanExpression class for checking primary key constraints
	BooleanExpression generatePrimaryCheckExpr(ArrayList<ColValTuple> cvTuple) {
		ArrayList<ColValTuple> pKeyTuple = ColValTuple.columnFilter(pKeys, cvTuple);
		
		return Relation.generateCheckExpr(pKeyTuple);
	}
	
	// Delete query
	public DBMessage delete(Database db, BooleanExpression where) throws MyException {
		int deleteCount = 0;
		int cancelCount = 0;

		ArrayList<ArrayList<Value>> searchResult;
		ArrayList<ArrayList<Value>> removeList = new ArrayList<ArrayList<Value>>();

		try {
			if (where == null) {
				searchResult = records;
			}
			else {
				searchResult = where.filter(this);
			}
		} catch (MyException e) {
			throw e;
		}

		for (ArrayList<Value> rec : searchResult) {
			
			ArrayList<ColValTuple> myPKey = new ArrayList<ColValTuple>();
			for (String pCol : pKeys) {
				myPKey.add(new ColValTuple(pCol, rec.get(getIndexByColumnName(pCol))));
			}
			
			// Integrity violation check
			boolean noViolation;

			noViolation = checkNoReferentialIntegrityViolation(db, myPKey);

			if (noViolation) {

				if (pKeys.size() > 0) {
					for (String refTable : referredTableList) {
						Relation refRel = db_search(db, refTable);
						refRel.cascadeDeletion(db, tableName, myPKey);
					}
				}

				removeList.add(rec);
				deleteCount++;
			}
			else {
				cancelCount++;
			}
		}
		
		for (ArrayList<Value> rec : removeList) {
			records.remove(rec);
		}
		
		return new DBMessage(MsgType.DeleteResult, deleteCount, cancelCount);
	}
	
	// Cascade Deletion
	void cascadeDeletion(Database db, String dTable, ArrayList<ColValTuple> cvList) {
		for (ForeignKeyConstraint fkc : fKeys) {
			if (fkc.refTable.equals(dTable)) {
				ArrayList<ColValTuple> myside = ColValTuple.columnFilter(fkc.referingKeys, cvList);
				for (int i = 0; i < fkc.referingKeys.size(); i++) {
					myside.get(i).columnName = fkc.foreignKeys.get(i);
				}
				
				BooleanExpression bxpr = Relation.generateCheckExpr(myside);
				ArrayList<ArrayList<Value>> searchResult = search(bxpr);
				
				for (ArrayList<Value> rec : searchResult) {
					for (String fk : fkc.foreignKeys) {
						rec.get(getIndexByColumnName(fk)).setNull();
					}
				}
			}
		}
		Relation.db_replace(db, this);
	}
	
	// Check if the record violates integrity constraints
	boolean checkViolateIntegrity(String dTable, ArrayList<ColValTuple> cvList) {
		for (ForeignKeyConstraint fkc : fKeys) {
			if (fkc.refTable.equals(dTable) && !fkc.nullable) {
				ArrayList<ColValTuple> myside = ColValTuple.columnFilter(fkc.referingKeys, cvList);
				for (int i = 0; i < fkc.referingKeys.size(); i++) {
					myside.get(i).columnName = fkc.foreignKeys.get(i);
				}
				
				BooleanExpression bxpr = Relation.generateCheckExpr(myside);
				ArrayList<ArrayList<Value>> searchResult = search(bxpr);
				
				if (searchResult.size() > 0) {
					return true;
				}
			}
		}
		return false;
	}
	
	private boolean checkNoReferentialIntegrityViolation(Database db, ArrayList<ColValTuple> myPKey) {
		
		if (myPKey.size() == 0) {
			return true;
		}
		
		boolean violate;
		for (String referedTable : referredTableList) {
			
			Relation referedRel = db_search(db, referedTable);

			violate = referedRel.checkViolateIntegrity(tableName, myPKey);
			
			if (violate) {
				return false;
			}
		}
		
		return true;
	}
	
	public ArrayList<ArrayList<Value>> search(BooleanExpression bxpr) {
		ArrayList<ArrayList<Value>> result = null;
		try {
			result = bxpr.filter(this);
		} catch (MyException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	// Select query
	DBMessage select(ArrayList<Rename> selected, BooleanExpression bxpr) {
		ArrayList<ArrayList<Value>> searchResult = null;	// Result from BooleanExpression.filter
		ArrayList<Integer> indexList = new ArrayList<Integer>();
		ArrayList<ArrayList<Value>> selectedResult = null;	// Reorganized searchResult
		
		try {
			if (bxpr == null) {
				searchResult = records;
			} else {
				searchResult = bxpr.filter(this);
			}
		
			selectedResult = selectColumn(selected, searchResult, indexList);
		} catch (MyException e) {
			return e.getDBMessage();
		}
		
		selectPrintResult(indexList, selected, selectedResult);
		return null;
	}
	
	/*
	 * Select column from filtered result
	 * Convert from searchResult to selectedResult (column ordering)
	 */
	private ArrayList<ArrayList<Value>> selectColumn(ArrayList<Rename> sList, ArrayList<ArrayList<Value>> sResult, ArrayList<Integer> iList) throws MyException {
		int schemaSize = schema.size();

		if (sList == null) {
			for (int i = 0; i < schemaSize; i++) {
				iList.add(i);
			}
			return sResult;
		}

		ArrayList<ArrayList<Value>> result = new ArrayList<ArrayList<Value>>();

		String searchPattern;
		for (Rename rename : sList) {
			searchPattern = (rename.tableName == null ? "" : rename.tableName + ".") + rename.columnName;
			int idx = getIndexByColumnName(searchPattern);
			if (idx < 0) {
				throw new MyException(MsgType.SelectColumnResolveError, searchPattern);
			}
			iList.add(getIndexByColumnName(searchPattern));
		}

		ArrayList<Value> entity;
		for (ArrayList<Value> rec : sResult) {
			entity = new ArrayList<Value>();
			for (int idx : iList) {
				entity.add(rec.get(idx));
			}
			result.add(entity);
		}

		return result;
	}

	/*
	 * Print select query result
	 * selectList : columns
	 * 
	 * This method checks all result and calculate proper box size for each column 
	 */
	private void selectPrintResult(ArrayList<Integer> idxList, ArrayList<Rename> selectList, ArrayList<ArrayList<Value>> selectedResult) {
		ArrayList<String> titleList = new ArrayList<String>();

		Integer[] length = new Integer[idxList.size()];

		for (int i = 0; i < idxList.size(); i++) {
			length[i] = 1;
		}

		if (selectList == null) {
			for (int i = 0; i < idxList.size(); i++) {
				titleList.add(schema.get(i).getName().toUpperCase());
				length[i] = MyCalc.max(length[i], titleList.get(i).length());
			}
		} else {
			for (int i = 0; i < selectList.size(); i++) {
				Rename rename = selectList.get(i);
				if (rename.newName != null) {
					titleList.add(rename.newName.toUpperCase());
				} else {
					titleList.add(rename.columnName.toUpperCase());
				}
				length[i] = MyCalc.max(length[i], titleList.get(i).length());
			}
		}

		int resultSize = selectedResult.size();
		int columnSize = titleList.size();

		for (int i = 0; i < columnSize; i++) {
			for (int j = 0; j < resultSize; j++) {
				length[i] = MyCalc.max(length[i], selectedResult.get(j).get(i).getLength());
			}
		}

		selectRealPrint(titleList, length, selectedResult);
	}

	// Actual printing method
	private static void selectRealPrint(ArrayList<String> title, Integer[] length,
			ArrayList<ArrayList<Value>> searchResult) {
		int columnNum = title.size();
		ArrayList<String> format = new ArrayList<String>();

		for (int i = 0; i < columnNum; i++) {
			String f = String.format(" %%-%ds ", length[i]);
			format.add(f);
		}

		String border = "+";
		for (int i = 0; i < columnNum; i++) {
			for (int j = 0; j < length[i] + 2; j++) {
				border = border + "-";
			}
			border = border + "+";
		}

		System.out.println(border);

		// Column Names
		System.out.print("|");
		for (int i = 0; i < columnNum; i++) {
			System.out.printf(format.get(i), title.get(i));
			System.out.print("|");
		}
		System.out.println();

		System.out.println(border);

		// Data

		for (int j = 0; j < searchResult.size(); j++) {
			System.out.print("|");
			for (int i = 0; i < columnNum; i++) {
				System.out.printf(format.get(i), searchResult.get(j).get(i));
				System.out.print("|");
			}
			System.out.println();
		}

		System.out.println(border);
	}

	private static BooleanExpression generateCheckExpr(ArrayList<ColValTuple> cvTuple) {
		Predicate p = new Predicate(cvTuple.get(0));
		BooleanNode node = new BooleanNode(p);
		
		for (int i = 1; i < cvTuple.size(); i++) {
			p = new Predicate(cvTuple.get(i));
			node = new BooleanNode('&', node, new BooleanNode(p));
		}
		
		return new BooleanExpression(node);
	}
	
	// Select query
	public static DBMessage selectQuery(Database db, ArrayList<Rename> selected, ArrayList<Rename> tables, BooleanExpression bxpr) {
		DBMessage msg;

		msg = selectCheckValidTableName(db, tables);
		if (msg != null) {
			return msg;
		}

		Relation cartesian = Relation.selectJoin(db, tables);
		
		msg = cartesian.select(selected, bxpr);
		
		if (msg != null) {
			return msg;
		}

		return null;
	}
	
	// Join two relations
	public static Relation join(Relation r1, String newTable1, Relation r2, String newTable2) {

		Relation result = new Relation("--result");
		ArrayList<ArrayList<Value>> newRecord = result.getRecords();
		ArrayList<Attribute> schema = result.getSchema();

		if (newTable1 == null) {
			newTable1 = r1.getTableName();
		}

		if (newTable2 == null) {
			newTable2 = r2.getTableName();
		}

		// Generate Schema
		ArrayList<Attribute> schema1 = r1.getSchema();
		ArrayList<Attribute> schema2 = r2.getSchema();
		Attribute newAttr;

		for (Attribute attr : schema1) {
			newAttr = attr.copyAttribute();
			newAttr.setFullName(newTable1);
			schema.add(newAttr);
		}
		for (Attribute attr : schema2) {
			newAttr = attr.copyAttribute();
			newAttr.setFullName(newTable2);
			schema.add(newAttr);
		}

		// Generate Record Table
		ArrayList<ArrayList<Value>> record1 = r1.getRecords();
		ArrayList<ArrayList<Value>> record2 = r2.getRecords();
		int rsize1 = record1.size();
		int rsize2 = record2.size();

		ArrayList<Value> newEntity;

		if (r1.isEmptyRelation()) {
			for (int j = 0; j < rsize2; j++) {
				newEntity = new ArrayList<Value>();
				newEntity.addAll(record2.get(j));
				newRecord.add(newEntity);
			}
			return result;
		}

		for (int i = 0; i < rsize1; i++) {
			for (int j = 0; j < rsize2; j++) {
				newEntity = new ArrayList<Value>();
				newEntity.addAll(record1.get(i));
				newEntity.addAll(record2.get(j));
				newRecord.add(newEntity);
			}
		}

		return result;
	}

	// check if table list from select query is valid or not
	private static DBMessage selectCheckValidTableName(Database db, ArrayList<Rename> tables) {
		ArrayList<String> newNameDupCheck = new ArrayList<String>();

		for (Rename r : tables) {
			Relation rel = Relation.db_search(db, r.tableName);
			if (rel == null) {
				return new DBMessage(MsgType.SelectTableExistenceError, r.tableName);
			}
			
			if (r.newName == null) continue;
			
			if (newNameDupCheck.contains(r.newName)) {
				return new DBMessage(MsgType.WhereAmbiguousReference);
			} else {
				newNameDupCheck.add(r.newName);
			}
		}
		return null;
	}

	// Generate one big table from select query
	private static Relation selectJoin(Database db, ArrayList<Rename> tables) {
		Relation result = new Relation(EMPTY_RELATION);
		Relation rtemp;

		for (Rename r : tables) {
			rtemp = Relation.db_search(db, r.tableName);
			result = Relation.join(result, null, rtemp, r.newName);
		}

		return result;
	}
	
	// Berkeley DB IO for Relation class
	public static void db_insert(Database db, Relation r) {
		DataManager.insert(db, r.getTableName(), DataManager.serialize(r));
	}

	public static void db_replace(Database db, Relation r) {
		DataManager.replace(db, r.getTableName(), DataManager.serialize(r));
	}

	public static Relation db_search(Database db, String table) {
		byte[] data = DataManager.search(db, table);
		Relation rel = null;
		if (data != null) {
			rel = (Relation) DataManager.deserialize(data);
		}
		return rel;
	}

	public static void db_delete(Database db, Relation r) {
		DataManager.delete(db, r.getTableName());
	}

}
