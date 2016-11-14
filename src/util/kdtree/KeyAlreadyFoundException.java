package util.kdtree;

public class KeyAlreadyFoundException extends RuntimeException {

    private static final long serialVersionUID = 1774207131386358008L;

    public KeyAlreadyFoundException() {
    }

    public KeyAlreadyFoundException(String arg0) {
        super(arg0);
    }

    public KeyAlreadyFoundException(Throwable arg0) {
        super(arg0);
    }

    public KeyAlreadyFoundException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

}

