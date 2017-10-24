package com.github.swapUniba.pulse.crowd.messagelabeller;

import com.github.frapontillo.pulse.crowd.data.entity.Message;

public class Main {
    public static void main(String[] args) {

        Message msg = new Message();
        msg.setSentiment(30.0);

        Message m = MessageLabellerPlugin.parseCondition(msg,"sentiment>=30","positivo");
        System.out.println(m.toString());
    }
}
