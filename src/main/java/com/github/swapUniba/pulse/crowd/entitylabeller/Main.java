package com.github.swapUniba.pulse.crowd.entitylabeller;

import com.github.frapontillo.pulse.crowd.data.entity.Category;
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
        tg.setText("grillo");
        tags.add(tg);

        Tag tg1 = new Tag();
        tg1.setText("testing_politica_class_m5s");
        tags.add(tg1);

        Category ct = new Category();
        ct.setText("Categoria:parlamentare");
        Category ct1 = new Category();
        ct1.setText("Categoria:deputato");
        Category ct2 = new Category();
        ct2.setText("Categoria:presidente");

        Set<Category> cats = new HashSet<>();
        cats.add(ct);
        cats.add(ct1);
        cats.add(ct2);

        tg.setCategories(cats);

        msg.setTags(tags);

        Entity m = EntityLabellerPlugin.parseCondition(msg,"politica","tags == testing_politica_class_m5s","5stelle");
        System.out.println(m.toString());



    }
}
