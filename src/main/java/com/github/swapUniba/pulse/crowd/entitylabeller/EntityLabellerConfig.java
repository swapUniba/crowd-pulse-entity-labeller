package com.github.swapUniba.pulse.crowd.entitylabeller;

import com.github.frapontillo.pulse.spi.IPluginConfig;
import com.github.frapontillo.pulse.spi.PluginConfigHelper;
import com.google.gson.JsonElement;

public class EntityLabellerConfig implements IPluginConfig<EntityLabellerConfig>{

    private String className;
    private String modelName;
    private String condition = "";

    @Override
    public EntityLabellerConfig buildFromJsonElement(JsonElement jsonElement) {
        return PluginConfigHelper.buildFromJson(jsonElement, EntityLabellerConfig.class);
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

    public String getModelName() {
        return modelName.toLowerCase();
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }
}
