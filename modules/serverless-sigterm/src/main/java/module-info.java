import co.elastic.elasticsearch.serverless.shutdown.SigtermHandlerProvider;

module org.elasticsearch.internal.sigterm {
    requires org.elasticsearch.server;
    requires org.apache.logging.log4j;
    requires org.elasticsearch.base;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.shutdown;

    exports co.elastic.elasticsearch.serverless.shutdown to org.elasticsearch.server;

    provides org.elasticsearch.node.internal.TerminationHandlerProvider with SigtermHandlerProvider;
}
