package com.github.swapUniba.pulse.crowd.messagelabeller;

import com.github.frapontillo.pulse.crowd.data.entity.Message;
import com.github.frapontillo.pulse.crowd.data.entity.Tag;
import com.github.frapontillo.pulse.crowd.data.entity.Token;
import com.github.frapontillo.pulse.spi.IPlugin;
import com.github.frapontillo.pulse.util.PulseLogger;
import org.apache.logging.log4j.Logger;
import rx.Observable;
import rx.Subscriber;
import rx.observers.SafeSubscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
                message = parseCondition(message,messageLabellerConfig.getCondition(),messageLabellerConfig.getClassName());
                subscriber.onNext(message);
            }
        });
    }

    public static Message parseCondition(Message message, String condition, String className) {

        String[] operators = {"==",">=","<=","!=","<",">"};
        String attribute = "";
        String operator = "";
        String value = "";

        List<String> customTags = message.getCustomTags();
        if (customTags == null) {
            customTags = new ArrayList<>();
        }
        customTags.add("trainingPredictedClass:"+className);


        for (String op : operators) {
            if (condition.contains(op)) {
                int ii = condition.indexOf(op);
                operator = op;
                String[] splitted = condition.split(op); //array di 2 posizioni: nome attributo, valore
                attribute = splitted[0].trim();
                value = splitted[1].trim();
                break;
            }
        }

        if (!condition.equalsIgnoreCase("")) {

            assert !attribute.equalsIgnoreCase("");
            assert !operator.equalsIgnoreCase("");
            assert !value.equalsIgnoreCase("");

            //SENTIMENT
            if (attribute.equalsIgnoreCase("sentiment")) {
                Double val = Double.parseDouble(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getSentiment() == val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getSentiment() >= val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getSentiment() <= val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getSentiment() > val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getSentiment() < val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getSentiment() != val) {
                        message.setCustomTags(customTags);
                    }
                }
            }
            //LATITUDE
            if (attribute.equalsIgnoreCase("latitude")) {
                Double val = Double.parseDouble(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getLatitude() == val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getLatitude() >= val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getLatitude() <= val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getLatitude() > val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getLatitude() < val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getLatitude() != val) {
                        message.setCustomTags(customTags);
                    }
                }
            }
            //LONGITUDE
            if (attribute.equalsIgnoreCase("longitude")) {
                Double val = Double.parseDouble(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getLongitude() == val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getLongitude() >= val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getLongitude() <= val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getLongitude() > val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getLongitude() < val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getLongitude() != val) {
                        message.setCustomTags(customTags);
                    }
                }
            }
            //FAVS
            if (attribute.equalsIgnoreCase("favs")) {
                int val = Integer.parseInt(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getFavs() == val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getFavs() >= val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getFavs() <= val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getFavs() > val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getFavs() < val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getFavs() != val) {
                        message.setCustomTags(customTags);
                    }
                }
            }
            //SHARES
            if (attribute.equalsIgnoreCase("shares")) {
                int val = Integer.parseInt(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getShares() == val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getShares() >= val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getShares() <= val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getShares() > val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getShares() < val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getShares() != val) {
                        message.setCustomTags(customTags);
                    }
                }
            }
            //NUMBER_CLUSTER
            if (attribute.equalsIgnoreCase("number_cluster")) {
                int val = Integer.parseInt(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getCluster() == val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getCluster() >= val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getCluster() <= val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getCluster() > val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getCluster() < val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getCluster() != val) {
                        message.setCustomTags(customTags);
                    }
                }
            }
            //CLUSTER_KMEANS
            if (attribute.equalsIgnoreCase("cluster_kmeans")) {
                int val = Integer.parseInt(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getClusterKmeans() == val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getClusterKmeans() >= val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getClusterKmeans() <= val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getClusterKmeans() > val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getClusterKmeans() < val) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getClusterKmeans() != val) {
                        message.setCustomTags(customTags);
                    }
                }
            }
            //LANGUAGE
            if (attribute.equalsIgnoreCase("language")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getLanguage().equalsIgnoreCase(val)) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (!message.getLanguage().equalsIgnoreCase(val)) {
                        message.setCustomTags(customTags);
                    }
                }
            }
            //PARENT
            if (attribute.equalsIgnoreCase("parent")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getParent().equalsIgnoreCase(val)) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (!message.getParent().equalsIgnoreCase(val)) {
                        message.setCustomTags(customTags);
                    }
                }
            }
            //FROMUSER
            if (attribute.equalsIgnoreCase("fromUser")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getFromUser().equalsIgnoreCase(val)) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (!message.getFromUser().equalsIgnoreCase(val)) {
                        message.setCustomTags(customTags);
                    }
                }
            }
            //SOURCE
            if (attribute.equalsIgnoreCase("source")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getSource().equalsIgnoreCase(val)) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (!message.getSource().equalsIgnoreCase(val)) {
                        message.setCustomTags(customTags);
                    }
                }
            }
            //TAGS
            if (attribute.equalsIgnoreCase("tags")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    Set<Tag> tags = message.getTags();
                    List<String> tagsText = tags.stream().map(t -> t.getText()).collect(Collectors.toList());
                    if (tagsText.contains(val)) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    Set<Tag> tags = message.getTags();
                    if (!tags.contains(val)) {
                        message.setCustomTags(customTags);
                    }
                }
            }
            //TOUSERS
            if (attribute.equalsIgnoreCase("tousers")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    List<String> toUsers = message.getToUsers();
                    if (toUsers.contains(val)) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    List<String> toUsers = message.getToUsers();
                    if (!toUsers.contains(val)) {
                        message.setCustomTags(customTags);
                    }
                }
            }
            //REFUSERS
            if (attribute.equalsIgnoreCase("refusers")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    List<String> refUsers = message.getRefUsers();
                    if (refUsers.contains(val)) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    List<String> refUsers = message.getRefUsers();
                    if (!refUsers.contains(val)) {
                        message.setCustomTags(customTags);
                    }
                }
            }
            //TOKENS
            if (attribute.equalsIgnoreCase("tokens")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    List<Token> tokens = message.getTokens();
                    List<String> tokensText = tokens.stream().map(t -> t.getText()).collect(Collectors.toList());
                    if (tokensText.contains(val)) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    List<Token> tokens = message.getTokens();
                    if (!tokens.contains(val)) {
                        message.setCustomTags(customTags);
                    }
                }
            }
            //CUSTOMTAGS
            if (attribute.equalsIgnoreCase("customtags")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    List<String> custom = message.getCustomTags();
                    if (custom.contains(val)) {
                        message.setCustomTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    List<String> custom = message.getCustomTags();
                    if (!custom.contains(val)) {
                        message.setCustomTags(customTags);
                    }
                }
            }
        }
        else {
            message.setCustomTags(customTags);
        }

        return message;

    }

}
