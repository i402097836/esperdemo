package com.es.esper.event;

import com.espertech.esper.client.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PersonMap
{
    public static void main(String[] args) {
        EPServiceProvider epService=EPServiceProviderManager.getDefaultProvider();
        EPAdministrator admin=epService.getEPAdministrator();

        Map<String,Object> person=new HashMap<String,Object>();
        person.put("name", String.class);
        person.put("Person.phones", int.class);
        person.put("children", List.class);
        person.put("phones", Map.class);

        admin.getConfiguration().addEventType("Person",person);

        String epl = "select phones('age'),children from Person where name='cjq'";

        EPStatement state = admin.createEPL(epl);
        state.addListener(new PersonMapListener());
        EPRuntime runtime = epService.getEPRuntime();

        Map<String,Object> person1=new HashMap<String,Object>();

        List<String> children=new ArrayList<String>();
        children.add("x");
        children.add("y");
        children.add("z");

        Map<String,Integer> phones=new HashMap<String,Integer>();
        phones.put("age", 123);
        phones.put("b", 234);

        person1.put("name","cjq");
        person1.put("Person.phones",12);
        person1.put("children", children);
        person1.put("phones", phones);

        runtime.sendEvent(person1, "Person");

    }
    static class PersonMapListener implements UpdateListener {

        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            if (newEvents != null) {
                Object age = newEvents[0].get("phones('age')");
               List children = (List) newEvents[0].get("children");
               // Map phones = (Map) newEvents[0].get("Person.phones");
                System.out.println("age is:"+age+",children="+children+",phone=");
            }
        }

    }
}