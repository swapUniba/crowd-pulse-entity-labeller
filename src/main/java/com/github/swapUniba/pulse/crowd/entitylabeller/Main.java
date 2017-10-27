package com.github.swapUniba.pulse.crowd.entitylabeller;

import com.github.frapontillo.pulse.crowd.data.entity.Entity;
import com.github.frapontillo.pulse.crowd.data.entity.Message;

public class Main {
    public static void main(String[] args) {

        Message msg = new Message();
        msg.setSentiment(30.0);
        msg.setFavs(1);

        Entity m = EntityLabellerPlugin.parseCondition(msg,"politica","fav>0","positivo");
        System.out.println(m.toString());
    }
}
