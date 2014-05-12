package com.pardot.rhombus.web.odata;

import org.odata4j.jersey.producer.server.ODataJerseyServer;

import javax.ws.rs.core.Application;

/**
 * Pardot, an ExactTarget company
 * User: Michael Frank
 * Date: 5/9/14
 */
public class RODataJerseyServer extends ODataJerseyServer {

	public RODataJerseyServer(String appBaseUri) {
		super(appBaseUri);
		this.addJerseyRequestFilter(com.sun.jersey.api.container.filter.LoggingFilter.class);
	}

	public RODataJerseyServer(String appBaseUri, Class<? extends Application> odataApp, Class<? extends Application> rootApp) {
		super(appBaseUri, odataApp, rootApp);
	}
}
