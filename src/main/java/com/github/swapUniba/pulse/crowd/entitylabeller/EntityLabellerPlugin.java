package com.github.swapUniba.pulse.crowd.entitylabeller;

import com.github.frapontillo.pulse.crowd.data.entity.*;
import com.github.frapontillo.pulse.spi.IPlugin;
import com.github.frapontillo.pulse.util.PulseLogger;
import org.apache.logging.log4j.Logger;
import rx.Observable;
import rx.Subscriber;
import rx.observers.SafeSubscriber;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class EntityLabellerPlugin extends IPlugin<Entity,Entity,EntityLabellerConfig> {

    private static final String PLUGIN_NAME = "entity-labeller";
    public static final Logger logger = PulseLogger.getLogger(EntityLabellerPlugin.class);

    @Override
    public String getName() {
        return PLUGIN_NAME;
    }

    @Override
    public EntityLabellerConfig getNewParameter() {
        return new EntityLabellerConfig();
    }

    @Override
    protected Observable.Operator<Entity, Entity> getOperator(EntityLabellerConfig messageLabellerConfig) {
        return subscriber -> new SafeSubscriber<>(new Subscriber<Entity>() {


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
            public void onNext(Entity entity) {
                entity = parseCondition(entity,messageLabellerConfig.getModelName(),messageLabellerConfig.getCondition(),messageLabellerConfig.getClassName());
                subscriber.onNext(entity);
            }
        });
    }

    public static Entity parseCondition(Entity entity, String modelName, String condition, String className) {

        Entity result = null;

        if (entity.getClass() == Message.class) {
            Message message = (Message)entity;
            result = parseMessageConditions(message,modelName,condition,className);
        }
        else if (entity.getClass() == Profile.class) {
            // metodo per parsing di profili con condizioni
        }

        return result;

    }

    private static Message parseMessageConditions(Message message, String modelName, String condition, String className) {

        String[] operators = {"==",">=","<=","!=","<",">"};
        String attribute = "";
        String operator = "";
        String value = "";

        modelName = modelName.toLowerCase();

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

            //logger.info("CONDIZIONE:" + attribute + operator + value);

            //SENTIMENT
            if (attribute.equalsIgnoreCase("sentiment")) {
                Double val = Double.parseDouble(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getSentiment() == val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getSentiment() >= val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getSentiment() <= val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getSentiment() > val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getSentiment() < val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getSentiment() != val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
            }
            //LATITUDE
            if (attribute.equalsIgnoreCase("latitude")) {
                Double val = Double.parseDouble(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getLatitude() == val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getLatitude() >= val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getLatitude() <= val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getLatitude() > val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getLatitude() < val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getLatitude() != val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
            }
            //LONGITUDE
            if (attribute.equalsIgnoreCase("longitude")) {
                Double val = Double.parseDouble(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getLongitude() == val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getLongitude() >= val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getLongitude() <= val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getLongitude() > val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getLongitude() < val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getLongitude() != val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
            }
            //FAVS
            if (attribute.equalsIgnoreCase("favs")) {
                int val = Integer.parseInt(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getFavs() == val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getFavs() >= val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getFavs() <= val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getFavs() > val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getFavs() < val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getFavs() != val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
            }
            //SHARES
            if (attribute.equalsIgnoreCase("shares")) {
                int val = Integer.parseInt(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getShares() == val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getShares() >= val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getShares() <= val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getShares() > val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getShares() < val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getShares() != val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
            }
            //NUMBER_CLUSTER
            if (attribute.equalsIgnoreCase("number_cluster")) {
                int val = Integer.parseInt(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getCluster() == val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getCluster() >= val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getCluster() <= val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getCluster() > val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getCluster() < val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getCluster() != val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
            }
            //CLUSTER_KMEANS
            if (attribute.equalsIgnoreCase("cluster_kmeans")) {
                int val = Integer.parseInt(value);
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getClusterKmeans() == val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase(">=")) {
                    if (message.getClusterKmeans() >= val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("<=")) {
                    if (message.getClusterKmeans() <= val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase(">")) {
                    if (message.getClusterKmeans() > val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("<")) {
                    if (message.getClusterKmeans() < val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (message.getClusterKmeans() != val) {
                        setClassToMessage(message,modelName,className);
                    }
                }
            }
            //LANGUAGE
            if (attribute.equalsIgnoreCase("language")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getLanguage().equalsIgnoreCase(val)) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (!message.getLanguage().equalsIgnoreCase(val)) {
                        setClassToMessage(message,modelName,className);
                    }
                }
            }
            //PARENT
            if (attribute.equalsIgnoreCase("parent")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getParent().equalsIgnoreCase(val)) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (!message.getParent().equalsIgnoreCase(val)) {
                        setClassToMessage(message,modelName,className);
                    }
                }
            }
            //FROMUSER
            if (attribute.equalsIgnoreCase("fromUser")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getFromUser().equalsIgnoreCase(val)) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (!message.getFromUser().equalsIgnoreCase(val)) {
                        setClassToMessage(message,modelName,className);
                    }
                }
            }
            //SOURCE
            if (attribute.equalsIgnoreCase("source")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    if (message.getSource().equalsIgnoreCase(val)) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    if (!message.getSource().equalsIgnoreCase(val)) {
                        setClassToMessage(message,modelName,className);
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
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    Set<Tag> tags = message.getTags();
                    if (!tags.contains(val)) {
                        setClassToMessage(message,modelName,className);
                    }
                }
            }
            //TOUSERS
            if (attribute.equalsIgnoreCase("tousers")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    List<String> toUsers = message.getToUsers();
                    if (toUsers.contains(val)) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    List<String> toUsers = message.getToUsers();
                    if (!toUsers.contains(val)) {
                        setClassToMessage(message,modelName,className);
                    }
                }
            }
            //REFUSERS
            if (attribute.equalsIgnoreCase("refusers")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    List<String> refUsers = message.getRefUsers();
                    if (refUsers.contains(val)) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    List<String> refUsers = message.getRefUsers();
                    if (!refUsers.contains(val)) {
                        setClassToMessage(message,modelName,className);
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
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    List<Token> tokens = message.getTokens();
                    if (!tokens.contains(val)) {
                        setClassToMessage(message,modelName,className);
                    }
                }
            }
            //CUSTOMTAGS
            if (attribute.equalsIgnoreCase("customtags")) {
                String val = value;
                if (operator.equalsIgnoreCase("==")) {
                    List<String> custom = message.getCustomTags();
                    if (custom.contains(val)) {
                        setClassToMessage(message,modelName,className);
                    }
                }
                if (operator.equalsIgnoreCase("!=")) {
                    List<String> custom = message.getCustomTags();
                    if (!custom.contains(val)) {
                        setClassToMessage(message,modelName,className);
                    }
                }
            }

        }
        else {
            setClassToMessage(message,modelName,className);
        }

        return message;
    }


    private static void setClassToMessage(Message message, String modelName, String className) {

        if (message.getTags() == null) {
            message.setTags(new HashSet<>());
        }
        Set<Tag> customTags = new HashSet<>(message.getTags());

        customTags.stream()
                .filter(p -> p.getText().toLowerCase().startsWith("training_" + modelName + "_class_"))
                .collect(Collectors.toList());

        Tag tag = new Tag();
        tag.setText("training_" + modelName + "_class_" + className);
        customTags.add(tag);

        message.setTags(customTags);

    }

}
