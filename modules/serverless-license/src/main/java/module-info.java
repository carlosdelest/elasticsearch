import co.elastic.elasticsearch.serverless.license.ServerlessLicenseService;

module org.elasticsearch.internal.license {

    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.xcontent;
    requires org.apache.logging.log4j;

    exports co.elastic.elasticsearch.serverless.license to org.elasticsearch.server;

    provides org.elasticsearch.license.internal.MutableLicenseService with ServerlessLicenseService;
}
