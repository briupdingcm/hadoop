package jbossOnYarn;


public interface JBossConstants {

    String JBOSS_VERSION = "jboss-as-7.1.1.Final";

    String JBOSS_DIST_PATH = "hdfs://192.168.150.128:9000/user/yarn/jboss/jboss-as-7.1.1.Final.tar.gz";

    String JBOSS_SYMLINK = "jboss";

    String JBOSS_YARN = "jboss-yarn";

    String JBOSS_MGT_REALM = "ManagementRealm";

    String JBOSS_CONTAINER_LOG_DIR = "/var/log/hadoop/yarn";

    String JBOSS_ON_YARN_APP = "JBossApp.jar";

    String COMMAND_CHAIN = " && ";
}
