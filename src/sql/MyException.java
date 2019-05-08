package sql;

public class MyException extends Exception {
	private static final long serialVersionUID = 1L;
	
	DBMessage msg;
	public MyException(DBMessage msg) {
		this.msg = msg;
	}
	public MyException(MsgType type) {
		msg = new DBMessage(type);
	}
	public MyException(MsgType type, String s) {
		msg = new DBMessage(type, s);
	}
	public MyException(MsgType type, int c) {
		msg = new DBMessage(type, c);
	}
	public DBMessage getDBMessage() {
		return msg;
	}
}
