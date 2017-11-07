package com.github.swapUniba.pulse.crowd.entitylabeller;

import com.github.frapontillo.pulse.spi.IPluginConfig;
import com.github.frapontillo.pulse.spi.PluginConfigHelper;
import com.google.gson.JsonElement;

public class EntityLabellerConfig implements IPluginConfig<EntityLabellerConfig>{

    private String className;
    private String modelName;
    private String conditions = "";

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

    public String getConditions() {
        return conditions;
    }

    public void setConditions(String conditions) {
        this.conditions = conditions;
    }

    public String getModelName() {
        return modelName.toLowerCase();
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }
}
