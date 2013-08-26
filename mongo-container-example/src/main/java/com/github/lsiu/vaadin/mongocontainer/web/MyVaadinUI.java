package com.github.lsiu.vaadin.mongocontainer.web;

import java.net.UnknownHostException;

import javax.servlet.annotation.WebServlet;

import com.github.lsiu.vaadin.mongo.MongoContainer;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.vaadin.annotations.Theme;
import com.vaadin.annotations.VaadinServletConfiguration;
import com.vaadin.server.VaadinRequest;
import com.vaadin.server.VaadinServlet;
import com.vaadin.ui.Table;
import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;

@Theme("mytheme")
@SuppressWarnings("serial")
public class MyVaadinUI extends UI
{

    @WebServlet(value = "/*", asyncSupported = true)
    @VaadinServletConfiguration(productionMode = false, ui = MyVaadinUI.class)
    //@VaadinServletConfiguration(productionMode = false, ui = MyVaadinUI.class, widgetset = "org.mongo.container.testweb.AppWidgetSet")
    public static class Servlet extends VaadinServlet {
    }
    
    private DBCollection collection;
    
    public MyVaadinUI() throws UnknownHostException {
    	MongoClient client = new MongoClient();
    	DB pcat = client.getDB("test");
    	collection = pcat.getCollection("restaurants");
    }

    @Override
    protected void init(VaadinRequest request) {
        final VerticalLayout layout = new VerticalLayout();
        layout.setMargin(true);
        setContent(layout);
        
        Table table = new Table();
        MongoContainer container = new MongoContainer(collection, "_id");
        table.setContainerDataSource(container);
        layout.addComponent(table);
    }

}
