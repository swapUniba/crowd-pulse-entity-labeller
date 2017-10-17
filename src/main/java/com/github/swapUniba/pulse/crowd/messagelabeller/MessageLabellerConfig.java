package com.github.swapUniba.pulse.crowd.messagelabeller;

import com.github.frapontillo.pulse.spi.IPluginConfig;
import com.github.frapontillo.pulse.spi.PluginConfigHelper;
import com.google.gson.JsonElement;

public class MessageLabellerConfig implements IPluginConfig<MessageLabellerConfig>{

    private String className;

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
}
