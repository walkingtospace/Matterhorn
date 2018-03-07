package common.message;

public interface AdminMessage {
    
    public enum OptType {
        INIT,
        START,
        STOP,
        ADD,
        REMOVE,
    }

    public enum OptStatus {
        INIT_SUCCESS,
        INIT_ERROR,
        START_SUCCESS,
        START_ERROR,
        STOP_SUCCESS,
        STOP_ERROR,
        ADD_SUCCESS,
        ADD_ERROR,
        REMOVE_SUCCESS,
        REMOVE_ERROR
    }

    public OptType getOptType();

    public OptStatus getOptStatus();
}


