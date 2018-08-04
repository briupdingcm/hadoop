package jbossOnYarn;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 在Yarn启动JBoss Application Master的客户端
 *
 * @author niuhy
 */
public class JBossClient {

    private static final Logger LOG = Logger.getLogger(JBossClient.class.getName());
    private final String appMasterMainClass = JBossApplicationMaster.class.getName();
    private Configuration config;
    //	与ResourceManager交互的客户端
    private YarnClient yarnClient;
    private String appName = "";
    private int amPriority = 0;
    private String amQueue = "";
    private int amMemory = 10241;
    private String appJar = "";
    private int shellCmdPriority = 0;

    private int containerMemory = 1024;
    private int numContainers = 2;

    private String adminUser;
    private String adminPassword;
    private String jbossAppUri;

    private String log4jPropFile = "";

    private boolean debugFlag = false;

    private Options opts;

    /**
     * 无参构造器
     */
    public JBossClient() {
        this(new YarnConfiguration());
    }

    /**
     * 含参构造器
     * 1)构建YarnClinet并初始化
     * 2)构建命令选项
     *
     * @param conf 配置信息对象
     */
    public JBossClient(Configuration conf) {
        this.config = conf;
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(config);

        opts = new Options();
        opts.addOption("appname", true, "应用程序的名字，默认值:JBoss on Yarn");
        opts.addOption("priority", true, "应用程序的优先级，默认值:0");
        opts.addOption("queue", true, "应用程序提交的RM队列");
        opts.addOption("timeout", true, "应用程序超时毫秒数");
        opts.addOption("master_memory", true, "运行AM请求的内存大小，单位MB");
        opts.addOption("jar", true, "包含AM的jar包");
        opts.addOption("container_memory", true, "运行该shell命令请求的内存大小，单位MB");
        opts.addOption("num_containers", true, "执行shell命令的container数量");
        opts.addOption("admin_user", true, "管理员用户名");
        opts.addOption("admin_password", true, "管理员密码");
        opts.addOption("log_properties", true, "log4j的配置文件");
        opts.addOption("debug", false, "打印调试信息");
        opts.addOption("help", false, "打印帮助信息");
    }

    /**
     * JBoss客户端程序入口
     *
     * @param args 命令行参数
     */
    public void main(String[] args) {
        boolean result = false;
        try {
            JBossClient client = new JBossClient();

            LOG.info("初始化JBossClient");

            boolean doRun;
            try {
                doRun = client.init(args);
                if (!doRun) {
                    System.exit(-1);
                }
            } catch (ParseException e) {
                // TODO Auto-generated catch block
                System.err.println(e.getLocalizedMessage());
                new HelpFormatter().printHelp("Client", opts);
                System.exit(-1);
            }

            result = client.run();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.log(Level.SEVERE, "运行JBossClient时出错！", e);
        }
        if (result) {
            LOG.info("应用程序执行成功！");
            System.exit(0);
        }

        LOG.log(Level.SEVERE, "应用程序执行失败！");
        System.exit(2);
    }

    /**
     * 解析命令行选项,并初始化JBossClient相应参数
     *
     * @param args
     * @return
     * @throws ParseException
     */
    private boolean init(String[] args) throws ParseException {
        // TODO Auto-generated method stub
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (cliParser.hasOption("help")) {
//			打印帮助信息
            new HelpFormatter().printHelp("Client", opts);
        }

        if (cliParser.hasOption("debug")) {
            debugFlag = true;
        }

//		初始化相应参数
        appName = cliParser.getOptionValue("appname", "JBoss on Yarn");
        amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        amQueue = cliParser.getOptionValue("queue", "default");
        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "10"));

        if (amMemory < 0) {
            throw new IllegalArgumentException("AM的内存分配值无效:\t" + amMemory);
        }
        if (!cliParser.hasOption("jar")) {
            throw new IllegalArgumentException(
                    "没有指定AM存在的jar文件");
        }
        appJar = cliParser.getOptionValue("jar");
        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
        numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
        adminUser = cliParser.getOptionValue("admin_user", "yarn");
        adminPassword = cliParser.getOptionValue("admin_password", "yarn");

        if (containerMemory < 0 || numContainers < 1) {
            throw new IllegalArgumentException(
                    "Container指定的内存值或Container指定的个数无效,"
                            + " container内存=\t" + containerMemory
                            + ", container数量=\t" + numContainers);
        }

        log4jPropFile = cliParser.getOptionValue("log_properties", "");

        return true;
    }

    private boolean run() throws YarnException, IOException {
        // TODO Auto-generated method stub
        LOG.info("开启YarnClinet服务");
        yarnClient.start();

        YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
        LOG.info("集群中 NodeManagers的数量：\t" + clusterMetrics.getNumNodeManagers());

        List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
        LOG.info("集群中的node信息");
        for (NodeReport node : clusterNodeReports) {
            LOG.info("Node节点信息：nodeId=" + node.getNodeId()
                    + ",nodeAddress=" + node.getHttpAddress()
                    + ",nodeRackName=" + node.getRackName()
                    + "nodeNumContainers=" + node.getNumContainers());
        }

        List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
        for (QueueUserACLInfo aclInfo : listAclInfo) {
            for (QueueACL userAcl : aclInfo.getUserAcls()) {
                LOG.info("队列的用户访问控制列表：" + ", queueName="
                        + aclInfo.getQueueName() + ", userAcl="
                        + userAcl.name());
            }
        }

//		创建应用程序
        YarnClientApplication app = yarnClient.createApplication();

        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        LOG.info("集群节点中最大可用内存(MB)： " + maxMem);

        if (amMemory > maxMem) {
            LOG.info("AM请求的内存容量超过集群最大能力,当前值=" + amMemory + ", 最大值=" + maxMem);
            amMemory = maxMem;
        }

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
//		通过ApplicationSubmission获取应用程序ID
        ApplicationId appId = appContext.getApplicationId();
//		设置应用程序名字
        appContext.setApplicationName(appName);

//		创建运行AM的Container的启动对象
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

//		设置启动AM的资源（AM的jar包或相关的配置文件）
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        LOG.info("从本地文件系统复制ApplicationMaster的jar包到HDFS");
        FileSystem fs = FileSystem.get(config);
        Path src = new Path(appJar);
        String pathSuffix = appName + File.separator + appId.getId() + File.separator + JBossConstants.JBOSS_ON_YARN_APP;
        Path dst = new Path(fs.getHomeDirectory() + pathSuffix);
        jbossAppUri = dst.toUri().toString();
        fs.copyFromLocalFile(false, true, src, dst);
        FileStatus destStatus = fs.getFileStatus(dst);

        LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
        amJarRsrc.setType(LocalResourceType.FILE);
        amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
        amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst));
        amJarRsrc.setTimestamp(destStatus.getModificationTime());
        amJarRsrc.setSize(destStatus.getLen());

        localResources.put(JBossConstants.JBOSS_ON_YARN_APP, amJarRsrc);

        if (!log4jPropFile.isEmpty()) {
            Path log4jSrc = new Path(log4jPropFile);
            Path log4jDst = new Path(fs.getHomeDirectory(), "log4j.props");
            fs.copyFromLocalFile(false, true, log4jSrc, log4jDst);
            FileStatus log4jFileStatus = fs.getFileStatus(log4jDst);

            LocalResource log4jRsrc = Records.newRecord(LocalResource.class);
            log4jRsrc.setType(LocalResourceType.FILE);
            log4jRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
            log4jRsrc.setResource(ConverterUtils.getYarnUrlFromPath(log4jDst));
            log4jRsrc.setTimestamp(log4jFileStatus.getModificationTime());
            log4jRsrc.setSize(log4jFileStatus.getLen());

            localResources.put("log4j.properties", log4jRsrc);
        }

        amContainer.setLocalResources(localResources);


        LOG.info("设置启动AM的环境变量");
        Map<String, String> env = new HashMap<>();
        StringBuffer classpathEnv = new StringBuffer(Environment.CLASSPATH.$()).append(File.pathSeparatorChar).append("./*");
        for (String s : config.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classpathEnv.append(File.pathSeparatorChar);
            classpathEnv.append(s.trim());
        }
        classpathEnv.append(File.pathSeparatorChar).append("./log4j.properties");

        if (config.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classpathEnv.append(File.pathSeparatorChar);
            classpathEnv.append(System.getProperty("java.class.path"));
        }
        env.put("CLASSPATH", classpathEnv.toString());
        amContainer.setEnvironment(env);

        LOG.info("设置启动AM的命令行");
        Vector<CharSequence> vargs = new Vector<>(30);

        vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
        vargs.add("-Xmx" + amMemory + "m");
        vargs.add(appMasterMainClass);
        vargs.add("--container_memory " + String.valueOf(containerMemory));
        vargs.add("--num_containers " + String.valueOf(numContainers));
        vargs.add("--priority " + String.valueOf(shellCmdPriority));
        vargs.add("--admin_user " + adminUser);
        vargs.add("--admin_password " + adminPassword);
        vargs.add("--jar " + jbossAppUri);

        if (debugFlag) {
            vargs.add("--debug");
        }
        vargs.add("1>" + JBossConstants.JBOSS_CONTAINER_LOG_DIR + "/JBossApplicationMaster.stdout");
        vargs.add("2>" + JBossConstants.JBOSS_CONTAINER_LOG_DIR + "/JBossApplicationMaster.stderr");

        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        LOG.info("完整的启动AM的命令:" + command.toString());
        List<String> commands = new ArrayList<>();
        commands.add(command.toString());
        amContainer.setCommands(commands);

        appContext.setAMContainerSpec(amContainer);

//		设置APP的资源需求（内存）
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(amMemory);
        appContext.setResource(capability);

//		设置队列
        appContext.setQueue(amQueue);
//		设置优先级
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(amPriority);
        appContext.setPriority(pri);

        LOG.info("提交应用程序到ASM");

        yarnClient.submitApplication(appContext);

        return monitorApplication(appId);
    }

    /**
     * 监控提交的APP的运行情况，如果应用超时kill该进程
     *
     * @param appId
     * @return
     * @throws YarnException
     * @throws IOException
     */
    private boolean monitorApplication(ApplicationId appId) throws YarnException, IOException {
        // TODO Auto-generated method stub
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            ApplicationReport report = yarnClient.getApplicationReport(appId);

            LOG.info("应用程序运行情况：appId=" + appId.getId()
                    + "\tclientToAMToken=" + report.getClientToAMToken()
                    + "\tappDiagnostics=" + report.getDiagnostics()
                    + "\tappMasterHost=" + report.getHost()
                    + "\tappQueue=" + report.getQueue()
                    + "\tappMasterRPCPort=" + report.getRpcPort()
                    + "\tappStartTime=" + report.getStartTime()
                    + "\tyarnAppState=" + report.getYarnApplicationState().toString()
                    + "\tdistributedFinalState=" + report.getFinalApplicationStatus().toString()
                    + "\tappTrackingUrl=" + report.getTrackingUrl()
                    + "\tappUser=" + report.getUser());

            YarnApplicationState appState = report.getYarnApplicationState();
            FinalApplicationStatus jbossStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == appState) {
                if (FinalApplicationStatus.SUCCEEDED == jbossStatus) {
                    LOG.info("应用程序执行完毕！");
                    return true;
                } else {
                    LOG.info("应用程序执行失败，yarnState=" + appState.toString()
                            + "JBASFinalStatus=" + jbossStatus.toString());
                    return false;
                }

            } else if (YarnApplicationState.KILLED == appState || YarnApplicationState.FAILED == appState) {
                LOG.info("应用程序未完成,yarnState=" + appState.toString()
                        + "JBASFinalStatus=" + jbossStatus.toString());
                return false;

            }
        }
    }


}
