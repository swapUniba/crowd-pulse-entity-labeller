package com.github.swapUniba.pulse.crowd.messagelabeller;

import com.github.frapontillo.pulse.spi.IPluginConfig;
import com.github.frapontillo.pulse.spi.PluginConfigHelper;
import com.google.gson.JsonElement;

public class MessageLabellerConfig implements IPluginConfig<MessageLabellerConfig>{

    private String className;
    private String condition = "";

    @Override
    public MessageLabellerConfig buildFromJsonElement(JsonElement jsonElement) {
        return PluginConfigHelper.buildFromJson(jsonElement, MessageLabellerConfig.class);
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }
}
