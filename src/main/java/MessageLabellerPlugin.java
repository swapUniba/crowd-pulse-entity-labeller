import com.github.frapontillo.pulse.crowd.data.entity.Message;
import com.github.frapontillo.pulse.spi.IPlugin;
import rx.Observable;

public class MessageLabellerPlugin extends IPlugin<Message,Message,MessageLabellerConfig> {

    @Override
    public String getName() {
        return null;
    }

    @Override
    public MessageLabellerConfig getNewParameter() {
        return null;
    }

    @Override
    protected Observable.Operator<Message, Message> getOperator(MessageLabellerConfig messageLabellerConfig) {
        return null;
    }
}
