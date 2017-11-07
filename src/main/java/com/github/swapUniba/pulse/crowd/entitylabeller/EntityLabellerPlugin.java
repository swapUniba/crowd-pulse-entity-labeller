package com.github.swapUniba.pulse.crowd.entitylabeller;

import com.github.frapontillo.pulse.crowd.data.entity.*;
import com.github.frapontillo.pulse.spi.IPlugin;
import com.github.frapontillo.pulse.util.PulseLogger;
import org.apache.logging.log4j.Logger;
import rx.Observable;
import rx.Subscriber;
import rx.observers.SafeSubscriber;

import java.util.*;
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
                entity = parseCondition(entity,messageLabellerConfig.getModelName(),messageLabellerConfig.getConditions(),messageLabellerConfig.getClassName());
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

    private static Message parseMessageConditions(Message message, String modelName, String cond, String className) {

        String[] operators = {"==",">=","<=","!=","<",">"};
        String attribute = "";
        String operator = "";
        String value = "";

        modelName = modelName.toLowerCase();

        String[] conditionsArray = cond.split("&&");
        List<String> conditions = Arrays.asList(conditionsArray);
        boolean[] satisfied = new boolean[conditions.size()];

        for (String condition : conditions) {


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

                //region Condizioni

                //SENTIMENT
                if (attribute.equalsIgnoreCase("sentiment")) {
                    Double val = Double.parseDouble(value);
                    if (operator.equalsIgnoreCase("==")) {
                        if (message.getSentiment() == val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase(">=")) {
                        if (message.getSentiment() >= val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("<=")) {
                        if (message.getSentiment() <= val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase(">")) {
                        if (message.getSentiment() > val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("<")) {
                        if (message.getSentiment() < val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        if (message.getSentiment() != val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }
                //LATITUDE
                if (attribute.equalsIgnoreCase("latitude")) {
                    Double val = Double.parseDouble(value);
                    if (operator.equalsIgnoreCase("==")) {
                        if (message.getLatitude() == val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase(">=")) {
                        if (message.getLatitude() >= val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("<=")) {
                        if (message.getLatitude() <= val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase(">")) {
                        if (message.getLatitude() > val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("<")) {
                        if (message.getLatitude() < val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        if (message.getLatitude() != val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }
                //LONGITUDE
                if (attribute.equalsIgnoreCase("longitude")) {
                    Double val = Double.parseDouble(value);
                    if (operator.equalsIgnoreCase("==")) {
                        if (message.getLongitude() == val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase(">=")) {
                        if (message.getLongitude() >= val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("<=")) {
                        if (message.getLongitude() <= val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase(">")) {
                        if (message.getLongitude() > val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("<")) {
                        if (message.getLongitude() < val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        if (message.getLongitude() != val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }
                //FAVS
                if (attribute.equalsIgnoreCase("favs")) {
                    int val = Integer.parseInt(value);
                    if (operator.equalsIgnoreCase("==")) {
                        if (message.getFavs() == val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase(">=")) {
                        if (message.getFavs() >= val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("<=")) {
                        if (message.getFavs() <= val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase(">")) {
                        if (message.getFavs() > val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("<")) {
                        if (message.getFavs() < val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        if (message.getFavs() != val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }
                //SHARES
                if (attribute.equalsIgnoreCase("shares")) {
                    int val = Integer.parseInt(value);
                    if (operator.equalsIgnoreCase("==")) {
                        if (message.getShares() == val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase(">=")) {
                        if (message.getShares() >= val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("<=")) {
                        if (message.getShares() <= val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase(">")) {
                        if (message.getShares() > val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("<")) {
                        if (message.getShares() < val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        if (message.getShares() != val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }
                //NUMBER_CLUSTER
                if (attribute.equalsIgnoreCase("number_cluster")) {
                    int val = Integer.parseInt(value);
                    if (operator.equalsIgnoreCase("==")) {
                        if (message.getCluster() == val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase(">=")) {
                        if (message.getCluster() >= val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("<=")) {
                        if (message.getCluster() <= val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase(">")) {
                        if (message.getCluster() > val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("<")) {
                        if (message.getCluster() < val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        if (message.getCluster() != val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }
                //CLUSTER_KMEANS
                if (attribute.equalsIgnoreCase("cluster_kmeans")) {
                    int val = Integer.parseInt(value);
                    if (operator.equalsIgnoreCase("==")) {
                        if (message.getClusterKmeans() == val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase(">=")) {
                        if (message.getClusterKmeans() >= val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("<=")) {
                        if (message.getClusterKmeans() <= val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase(">")) {
                        if (message.getClusterKmeans() > val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("<")) {
                        if (message.getClusterKmeans() < val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        if (message.getClusterKmeans() != val) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }
                //LANGUAGE
                if (attribute.equalsIgnoreCase("language")) {
                    String val = value;
                    if (operator.equalsIgnoreCase("==")) {
                        if (message.getLanguage().equalsIgnoreCase(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        if (!message.getLanguage().equalsIgnoreCase(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }
                //oId
                if (attribute.equalsIgnoreCase("oId")) {
                    String val = value;
                    if (operator.equalsIgnoreCase("==")) {
                        if (message.getoId().equals(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        if (!message.getoId().equals(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }
                //PARENT
                if (attribute.equalsIgnoreCase("parent")) {
                    String val = value;
                    if (operator.equalsIgnoreCase("==")) {
                        if (message.getParent().equalsIgnoreCase(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        if (!message.getParent().equalsIgnoreCase(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }
                //FROMUSER
                if (attribute.equalsIgnoreCase("fromUser")) {
                    String val = value;
                    if (operator.equalsIgnoreCase("==")) {
                        if (message.getFromUser().equalsIgnoreCase(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        if (!message.getFromUser().equalsIgnoreCase(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }
                //SOURCE
                if (attribute.equalsIgnoreCase("source")) {
                    String val = value;
                    if (operator.equalsIgnoreCase("==")) {
                        if (message.getSource().equalsIgnoreCase(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        if (!message.getSource().equalsIgnoreCase(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }
                //TAGS
                if (attribute.equalsIgnoreCase("tags")) {
                    String val = value;
                    if (operator.equalsIgnoreCase("==")) {
                        Set<Tag> tags = message.getTags();
                        if (tags != null) {
                            List<String> tagsText = tags.stream().map(t -> t.getText()).collect(Collectors.toList());
                            if (tagsText.contains(val)) {
                                satisfied[conditions.indexOf(condition)] = true;
                            }
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        Set<Tag> tags = message.getTags();
                        List<String> tagsText = tags.stream().map(t -> t.getText()).collect(Collectors.toList());
                        if (!tagsText.contains(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }
                //TOUSERS
                if (attribute.equalsIgnoreCase("tousers")) {
                    String val = value;
                    if (operator.equalsIgnoreCase("==")) {
                        List<String> toUsers = message.getToUsers();
                        if (toUsers.contains(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        List<String> toUsers = message.getToUsers();
                        if (!toUsers.contains(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }
                //REFUSERS
                if (attribute.equalsIgnoreCase("refusers")) {
                    String val = value;
                    if (operator.equalsIgnoreCase("==")) {
                        List<String> refUsers = message.getRefUsers();
                        if (refUsers.contains(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        List<String> refUsers = message.getRefUsers();
                        if (!refUsers.contains(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
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
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        List<Token> tokens = message.getTokens();
                        List<String> tokensText = tokens.stream().map(t -> t.getText()).collect(Collectors.toList());
                        if (!tokensText.contains(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }
                //CUSTOMTAGS
                if (attribute.equalsIgnoreCase("customtags")) {
                    String val = value;
                    if (operator.equalsIgnoreCase("==")) {
                        List<String> custom = message.getCustomTags();
                        if (custom.contains(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        List<String> custom = message.getCustomTags();
                        if (!custom.contains(val)) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }
                //CATEGORIES
                if (attribute.equalsIgnoreCase("categories")) {
                    String val = value;
                    if (operator.equalsIgnoreCase("==")) {
                        Set<Tag> tags = message.getTags();
                        if (tags != null) {
                              TAG: for (Tag tg : tags) {
                                  if (tg.getCategories() != null) {
                                      CATEGORY: for (Category cg : tg.getCategories()) {
                                          if (!cg.isStopWord()) {
                                              String[] categ = cg.getText().split(":");
                                              if (categ[1].toLowerCase().contains(val.toLowerCase())) {
                                                  satisfied[conditions.indexOf(condition)] = true;
                                                  break TAG;
                                              }
                                          }
                                      }
                                  }
                              }
                        }
                    }
                    if (operator.equalsIgnoreCase("!=")) {
                        boolean present = false;
                        Set<Tag> tags = message.getTags();
                        if (tags != null) {
                            TAG: for (Tag tg : tags) {
                                if (tg.getCategories() != null) {
                                    CATEGORY: for (Category cg : tg.getCategories()) {
                                        if (!cg.isStopWord()) {
                                            String[] categ = cg.getText().split(":");
                                            if (categ[1].toLowerCase().contains(val.toLowerCase())) {
                                                present = true;
                                                break TAG;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if (!present) {
                            satisfied[conditions.indexOf(condition)] = true;
                        }
                    }
                }

            }
            else {
                satisfied[conditions.indexOf(condition)] = true;
            }

            //endregion
        }


        boolean allSatisfied = true;

        for (boolean b : satisfied) {
            if (!b) {
                allSatisfied = false;
                break;
            }
        }

        if (allSatisfied) {
            setClassToMessage(message,modelName,className);
        }

        return message;
    }


    private static void setClassToMessage(Message message, String modelName, String className) {

        if (message.getTags() == null) {
            message.setTags(new HashSet<>());
        }
        Set<Tag> customTags = new HashSet<>(message.getTags());

        customTags = customTags.stream()
                    .filter(p -> !p.getText().toLowerCase().startsWith("training_" + modelName.toLowerCase() + "_class_"))
                    .collect(Collectors.toSet());

        Tag tag = new Tag();
        tag.setText("training_" + modelName + "_class_" + className);
        customTags.add(tag);

        message.setTags(customTags);

    }

}
