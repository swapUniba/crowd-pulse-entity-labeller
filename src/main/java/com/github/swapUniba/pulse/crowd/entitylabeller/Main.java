package com.github.swapUniba.pulse.crowd.entitylabeller;

import com.github.frapontillo.pulse.crowd.data.entity.Entity;
import com.github.frapontillo.pulse.crowd.data.entity.Message;
import com.github.frapontillo.pulse.crowd.data.entity.Tag;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Main {
    public static void main(String[] args) {

        Message msg = new Message();
        msg.setSentiment(30.0);
        msg.setFavs(1);

        Set<Tag> tags = new HashSet<>();

        Tag tg = new Tag();
        tg.setText("m5s");
        tags.add(tg);
        msg.setTags(tags);

        Entity m = EntityLabellerPlugin.parseCondition(msg,"politica","tags == m5s","5stelle");
        System.out.println(m.toString());



    }
}
