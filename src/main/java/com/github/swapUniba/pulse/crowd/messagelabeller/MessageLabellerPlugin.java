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
import java.util.HashSet;
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

        if (message.getTags() == null) {
            message.setTags(new HashSet<>());
        }
        Set<Tag> customTags = new HashSet<>(message.getTags());
        Tag tag = new Tag();
        tag.setText("trainingPredictedClass_" + className);
        customTags.add(tag);

        for (String op : operators) {
            if (condition.contains(op)) {
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

            logger.info("CONDIZIONE:" + attribute + operator + value);

            //SENTIMENT
            if (attribute.equalsIgnoreCase("sentiment")) {
                Double val = Double.parseDouble(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getSentiment() == val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getSentiment() >= val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getSentiment() <= val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getSentiment() > val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getSentiment() < val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getSentiment() != val) {
                        message.setTags(customTags);
                    }
                }
            }
            //LATITUDE
            if (attribute.equalsIgnoreCase("latitude")) {
                Double val = Double.parseDouble(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getLatitude() == val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getLatitude() >= val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getLatitude() <= val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getLatitude() > val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getLatitude() < val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getLatitude() != val) {
                        message.setTags(customTags);
                    }
                }
            }
            //LONGITUDE
            if (attribute.equalsIgnoreCase("longitude")) {
                Double val = Double.parseDouble(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getLongitude() == val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getLongitude() >= val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getLongitude() <= val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getLongitude() > val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getLongitude() < val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getLongitude() != val) {
                        message.setTags(customTags);
                    }
                }
            }
            //FAVS
            if (attribute.equalsIgnoreCase("favs")) {
                int val = Integer.parseInt(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getFavs() == val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getFavs() >= val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getFavs() <= val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getFavs() > val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getFavs() < val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getFavs() != val) {
                        message.setTags(customTags);
                    }
                }
            }
            //SHARES
            if (attribute.equalsIgnoreCase("shares")) {
                int val = Integer.parseInt(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getShares() == val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getShares() >= val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getShares() <= val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getShares() > val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getShares() < val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getShares() != val) {
                        message.setTags(customTags);
                    }
                }
            }
            //NUMBER_CLUSTER
            if (attribute.equalsIgnoreCase("number_cluster")) {
                int val = Integer.parseInt(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getCluster() == val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getCluster() >= val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getCluster() <= val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getCluster() > val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getCluster() < val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getCluster() != val) {
                        message.setTags(customTags);
                    }
                }
            }
            //CLUSTER_KMEANS
            if (attribute.equalsIgnoreCase("cluster_kmeans")) {
                int val = Integer.parseInt(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getClusterKmeans() == val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getClusterKmeans() >= val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getClusterKmeans() <= val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getClusterKmeans() > val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getClusterKmeans() < val) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getClusterKmeans() != val) {
                        message.setTags(customTags);
                    }
                }
            }
            //LANGUAGE
            if (attribute.equalsIgnoreCase("language")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getLanguage().equalsIgnoreCase(val)) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (!message.getLanguage().equalsIgnoreCase(val)) {
                        message.setTags(customTags);
                    }
                }
            }
            //PARENT
            if (attribute.equalsIgnoreCase("parent")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getParent().equalsIgnoreCase(val)) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (!message.getParent().equalsIgnoreCase(val)) {
                        message.setTags(customTags);
                    }
                }
            }
            //FROMUSER
            if (attribute.equalsIgnoreCase("fromUser")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getFromUser().equalsIgnoreCase(val)) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (!message.getFromUser().equalsIgnoreCase(val)) {
                        message.setTags(customTags);
                    }
                }
            }
            //SOURCE
            if (attribute.equalsIgnoreCase("source")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getSource().equalsIgnoreCase(val)) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (!message.getSource().equalsIgnoreCase(val)) {
                        message.setTags(customTags);
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
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    Set<Tag> tags = message.getTags();
                    if (!tags.contains(val)) {
                        message.setTags(customTags);
                    }
                }
            }
            //TOUSERS
            if (attribute.equalsIgnoreCase("tousers")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    List<String> toUsers = message.getToUsers();
                    if (toUsers.contains(val)) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    List<String> toUsers = message.getToUsers();
                    if (!toUsers.contains(val)) {
                        message.setTags(customTags);
                    }
                }
            }
            //REFUSERS
            if (attribute.equalsIgnoreCase("refusers")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    List<String> refUsers = message.getRefUsers();
                    if (refUsers.contains(val)) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    List<String> refUsers = message.getRefUsers();
                    if (!refUsers.contains(val)) {
                        message.setTags(customTags);
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
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    List<Token> tokens = message.getTokens();
                    if (!tokens.contains(val)) {
                        message.setTags(customTags);
                    }
                }
            }
            //CUSTOMTAGS
            if (attribute.equalsIgnoreCase("customtags")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    List<String> custom = message.getCustomTags();
                    if (custom.contains(val)) {
                        message.setTags(customTags);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    List<String> custom = message.getCustomTags();
                    if (!custom.contains(val)) {
                        message.setTags(customTags);
                    }
                }
            }

        }
        else {
            message.setTags(customTags);
        }

        return message;

    }

}
