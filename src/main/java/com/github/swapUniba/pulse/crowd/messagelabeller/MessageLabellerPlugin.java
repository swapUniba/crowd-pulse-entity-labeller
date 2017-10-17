package com.github.swapUniba.pulse.crowd.messagelabeller;

import com.github.frapontillo.pulse.crowd.data.entity.Message;
import com.github.frapontillo.pulse.spi.IPlugin;
import com.github.frapontillo.pulse.util.PulseLogger;
import org.apache.logging.log4j.Logger;
import rx.Observable;
import rx.Subscriber;
import rx.observers.SafeSubscriber;

public class MessageLabellerPlugin extends IPlugin<Message,Message,MessageLabellerConfig> {

    private static final String PLUGIN_NAME = "message-labeller";
    public static final Logger logger = PulseLogger.getLogger(MessageLabellerPlugin.class);

    @Override
    public String getName() {
        return PLUGIN_NAME;
    }

    @Override
    public MessageLabellerConfig getNewParameter() {
        return new MessageLabellerConfig();
    }

    @Override
    protected Observable.Operator<Message, Message> getOperator(MessageLabellerConfig messageLabellerConfig) {
        return subscriber -> new SafeSubscriber<>(new Subscriber<Message>() {


            @Override
            public void onCompleted() {
                subscriber.onCompleted();
            }

            @Override
            public void onError(Throwable e) {
                logger.error("ERRORE:" + e.toString());
                subscriber.onError(e);
            }

            @Override
            public void onNext(Message message) {
                message.setParent(messageLabellerConfig.getClassName());
                subscriber.onNext(message);
            }
        });
    }
}