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
}

