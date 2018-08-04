package jbossOnYarn;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JBossApplicationMaster {
    private static final Logger LOG = Logger.getLogger(JBossApplicationMaster.class.getName());

    private Configuration config;
    @SuppressWarnings("rawtypes")
    private AMRMClientAsync resourceManager;
    private NMClientAsync nmClient;
    private NMCallbackHandler containerListener;

    private ApplicationAttemptId appAttemptID;
    private String appMasterHostname = "";
    private int appMasterRpcPort = 0;
    private String appMasterTrackingUrl = "";

    private int numTotalContainers = 2;
    private int containerMemory = 1024;
    private int requestPriority;

    private String adminUser;
    private String adminPassword;

    private AtomicInteger numCompletedContainers = new AtomicInteger();
    private AtomicInteger numAllocatedContainers = new AtomicInteger();
    private AtomicInteger numFailedContainers = new AtomicInteger();
    private AtomicInteger numRequestedContainers = new AtomicInteger();

    private Map<String, String> shellEnv = new HashMap<>();

    private String jbossHome;
    private String appJar;
    private String domainController;

    private volatile boolean done;
    private volatile boolean success;

    private List<Thread> launchThreads = new ArrayList<>();

    public JBossApplicationMaster() {
        config = new YarnConfiguration();
    }

    /**
     * JBoss AM启动入口
     *
     * @param args
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            //		创建JBossApplicationMaster实例并初始化
            JBossApplicationMaster appMaster = new JBossApplicationMaster();
            LOG.info("初始化JBossApplicationMaster");
            boolean doRun = appMaster.init(args);
            if (!doRun) {
                System.exit(0);
            }
            //		运行AM
            result = appMaster.run();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.log(Level.SEVERE, "JBossApplicationMaster运行时报错！");
            System.exit(1);
        }

        if (result) {
            LOG.info("JBoss AM程序执行成功！");
            System.exit(0);
        } else {
            LOG.info("JBoss AM程序执行失败！");
            System.exit(2);
        }
    }

    /**
     * 配置和解析命令行选项
     *
     * @param args
     * @return
     * @throws ParseException
     */
    public boolean init(String[] args) throws ParseException {
        // TODO Auto-generated method stub
//		命令选项设置
        Options opts = new Options();
        opts.addOption("app_attempt_id", true, "APP Attemp ID,仅供测试时使用");
        opts.addOption("admin_user", true, "管理员用户名");
        opts.addOption("admin_password", true, "管理员密码");
        opts.addOption("container_memory", true, "运行Shell命令的Container内存大小（MB）");
        opts.addOption("num_containers", true, "shell命令需要的Container数量");
        opts.addOption("jar", true, "包含应用程序的jar文件");
        opts.addOption("priority", true, "应用程序调用的优先级");
        opts.addOption("debug", false, "打印调试信息");
        opts.addOption("help", false, "打印帮助信息");
//		命令选项解析
        CommandLine cliParser = new GnuParser().parse(opts, args);
        if (args.length == 0) {
            printUsage(opts);
            throw new IllegalArgumentException("JBossApplicationMaster无初始化参数");
        }

        if (cliParser.hasOption("help")) {
            printUsage(opts);
            return false;
        }

        if (cliParser.hasOption("debug")) {
            dumpOutDebugInfo();
        }
        Map<String, String> envs = System.getenv();
        ContainerId containerId = ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name()));

        if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
            if (cliParser.hasOption("app_attempt_id")) {
                String appIdStr = cliParser.getOptionValue("app_attempt_id");
                appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
            } else {
                throw new IllegalArgumentException("App Attempt Id没有设置在环境变量中");
            }
        } else {
            containerId = ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name()));
            appAttemptID = containerId.getApplicationAttemptId();
        }

        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + "在环境变量中未设置");
        }
        if (!envs.containsKey(Environment.NM_HOST.name())) {
            throw new RuntimeException(Environment.NM_HOST.name() + "在环境变量中未设置");
        }
        if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException(Environment.NM_HTTP_PORT.name() + "在环境变量中未设置");
        }
        if (!envs.containsKey(Environment.NM_PORT.name())) {
            throw new RuntimeException(Environment.NM_PORT.name() + "在环境变量中未设置");
        }

        LOG.info("app的AM信息:appId=" + appAttemptID.getApplicationId().getId()
                + "\tclustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp()
                + "\tattemptId=" + appAttemptID.getAttemptId());


        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "1024"));
        numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));

        adminUser = cliParser.getOptionValue("adminUser", "yarn");
        adminPassword = cliParser.getOptionValue("admin_password", "yarn");
        appJar = cliParser.getOptionValue("jar");

        if (numTotalContainers <= 0) {
            throw new IllegalArgumentException("container数量不能小于0");
        }
        requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));

        return true;
    }


    @SuppressWarnings("unchecked")
    public boolean run() throws YarnException, IOException {
        // TODO Auto-generated method stub
        LOG.info("启动JBossApplicationMaster");
//		启动与ResourceManager通信的客户端
        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        resourceManager = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        resourceManager.init(config);
        resourceManager.start();

//		启动与NodeManager通信的客户端
        containerListener = new NMCallbackHandler();
        nmClient = new NMClientAsyncImpl(containerListener);
        nmClient.init(config);
        nmClient.start();

//		向ResourceManager注册
        RegisterApplicationMasterResponse response = resourceManager.registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);

        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("集群中最大可分配内存（MB）:" + maxMem);

        if (containerMemory > maxMem) {
            LOG.info("设置的Container内存超过可分配的最大内存：maxMem=" + maxMem + "\tcontainerMemory=" + containerMemory);
            containerMemory = maxMem;
        }
//		请求Container资源
        for (int i = 0; i < numTotalContainers; ++i) {
            ContainerRequest containerAsk = setupContainerAskForRm();
            resourceManager.addContainerRequest(containerAsk);
        }
        numRequestedContainers.set(numTotalContainers);

        while (!done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        finish();
        return success;
    }

    /**
     * 应用执行完毕处理
     */
    private void finish() {
        // TODO Auto-generated method stub
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                LOG.info("加入线程是异常: " + e.getMessage());
                e.printStackTrace();
            }
        }

        LOG.info("应用完成，关闭与NodeManager通信的客户端");
        nmClient.stop();

        LOG.info("应用完成，通知RM");
        FinalApplicationStatus appStatus;
        String appMessage = null;
        success = true;

        if (numFailedContainers.get() == 0 && numCompletedContainers.get() == numTotalContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "统计报告：total=" + numTotalContainers
                    + "\tcompleted=" + numCompletedContainers.get()
                    + "\tallocated=" + numAllocatedContainers.get()
                    + "\tfailed=" + numFailedContainers.get();
            success = false;
        }

        try {
//			通知ResourceManager注销ApplicationMaster
            resourceManager.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException e) {
            // TODO Auto-generated catch block
            LOG.log(Level.SEVERE, "注销APP失败", e);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            LOG.log(Level.SEVERE, "注销APP失败", e);
        }
        done = true;
//		关闭与ResourceManager通信的客户端
        resourceManager.stop();

    }

    /**
     * 设置向RM请求container的请求信息
     *
     * @return
     */
    private ContainerRequest setupContainerAskForRm() {
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(requestPriority);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(containerMemory);
        capability.setVirtualCores(2);

        ContainerRequest request = new ContainerRequest(capability, null, null, pri);
        LOG.info("Container请求信息：" + request.toString());
        return request;
    }

    /**
     * 打印帮助信息
     *
     * @param opts
     */
    private void printUsage(Options opts) {
        new HelpFormatter().printHelp("JBossApplicationMaster", opts);
    }

    /**
     * 打印调试信息
     */
    private void dumpOutDebugInfo() {
        // TODO Auto-generated method stub
        LOG.info("=======调试信息========");
        Map<String, String> envs = System.getenv();
        for (Map.Entry<String, String> env : envs.entrySet()) {
            LOG.info("System env: key=" + env.getKey() + ", val="
                    + env.getValue());
            System.out.println("System env: key=" + env.getKey() + ", val="
                    + env.getValue());
        }

        String cmd = "ls -al";
        Runtime run = Runtime.getRuntime();
        Process pr = null;
        try {
            pr = run.exec(cmd);
            pr.waitFor();

            BufferedReader buf = new BufferedReader(new InputStreamReader(
                    pr.getInputStream()));
            String line = "";
            while ((line = buf.readLine()) != null) {
                LOG.info("System CWD content: " + line);
                System.out.println("System CWD content: " + line);
            }
            buf.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 监听来自ResourceManager事件的回调函数处理器
     *
     * @author niuhy
     */
    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

        /**
         * 调用时机:ResourceManager为Application返回的的心跳应答 中包含完成的Container信息
         * 若其中还包含新分配的Container信息，则在onContainersAllocated之前
         *
         * @param statuses 完成的Container信息
         */
        @SuppressWarnings("unchecked")
        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            // TODO Auto-generated method stub
            LOG.info("RM响应完成Container的信息, completedCnt="
                    + completedContainers.size());
//			统计Container申请情况
            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info("Container的状态 containerID="
                        + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus="
                        + containerStatus.getExitStatus() + ", diagnostics="
                        + containerStatus.getDiagnostics());

                assert (containerStatus.getState() == ContainerState.COMPLETE);

                int exitStatus = containerStatus.getExitStatus();
                if (0 != exitStatus) {
                    if (ContainerExitStatus.ABORTED != exitStatus) {
                        numCompletedContainers.incrementAndGet();
                        numFailedContainers.incrementAndGet();
                    } else {
                        numAllocatedContainers.decrementAndGet();
                        numRequestedContainers.decrementAndGet();
                    }
                } else {
                    numCompletedContainers.incrementAndGet();
                    LOG.info("Container completed successfully."
                            + ", containerId="
                            + containerStatus.getContainerId());
                }
            }

            int askCount = numTotalContainers - numRequestedContainers.get();
            numRequestedContainers.addAndGet(askCount);
//			重新申请分配失败的Container
            if (askCount > 0) {
                for (int i = 0; i < askCount; ++i) {
                    ContainerRequest containerAsk = setupContainerAskForRm();
                    resourceManager.addContainerRequest(containerAsk);
                }
            }
//			Container申请完毕
            if (numCompletedContainers.get() == numTotalContainers) {
                done = true;
            }
        }

        /**
         * 调用时机：ResourceManager为Application返回的的心跳应答中包含新分配的Container信息
         * 若其中还包含完成的的Container信息，则在onContainersCompleted之后
         *
         * @param containers 新分配的Container信息
         */
        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            // TODO Auto-generated method stub
            LOG.info("RM响应分配Contaner的信息, allocatedCnt="
                    + allocatedContainers.size());
            numAllocatedContainers.addAndGet(allocatedContainers.size());
//			查看Container信息,并启动相应的Container
            for (Container allocatedContainer : allocatedContainers) {
                LOG.info("Launching shell command on a new container."
                        + ", containerId=" + allocatedContainer.getId()
                        + ", containerNode="
                        + allocatedContainer.getNodeId().getHost() + ":"
                        + allocatedContainer.getNodeId().getPort()
                        + ", containerNodeURI="
                        + allocatedContainer.getNodeHttpAddress()
                        + ", containerResourceMemory"
                        + allocatedContainer.getResource().getMemory());

//				启动相应的Container
                LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(
                        allocatedContainer, containerListener);
                Thread launchThread = new Thread(runnableLaunchContainer);

                launchThreads.add(launchThread);
                launchThread.start();
            }
        }

        /**
         * 调用时机：ResourceManager通知ApplicationMaster停止运行
         */
        @Override
        public void onShutdownRequest() {
            // TODO Auto-generated method stub
            done = true;
        }

        /**
         * 调用时机:ResourceManager管理的节点发生变化
         */
        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
            // TODO Auto-generated method stub

        }

        @Override
        public float getProgress() {
            // TODO Auto-generated method stub
            float progress = (float) numCompletedContainers.get()
                    / numTotalContainers;
            return progress;
        }

        /**
         * 调用时机：任何异常出现的时候
         */
        @Override
        public void onError(Throwable e) {
            // TODO Auto-generated method stub
            done = true;
            resourceManager.stop();
        }

    }

    /**
     * 监听来自NodeManager事件的回调函数处理器
     *
     * @author niuhy
     */
    private class NMCallbackHandler implements NMClientAsync.CallbackHandler {
        private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<>();

        public void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
            LOG.info("Callback container id : " + containerId.toString());

            if (containers.size() == 1) {
                domainController = container.getNodeId().getHost();
            }
        }

        public int getContainerCount() {
            return containers.size();
        }

        /**
         * 启动时机:当NodeManager接受到启动Container请求时
         */
        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
            // TODO Auto-generated method stub
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("成功启动Container: " + containerId);
            }
            Container container = containers.get(containerId);
            if (container != null) {
                nmClient.getContainerStatusAsync(containerId,
                        container.getNodeId());
            }
        }

        /**
         * 调用时机:当NodeManager应答Container当前状态时调用
         */
        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
            // TODO Auto-generated method stub
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Container的状态信息: id=" + containerId + ", status="
                        + containerStatus);
            }
        }

        /**
         * 调用时机:当NodeManager应答Container已停止时调用
         */
        @Override
        public void onContainerStopped(ContainerId containerId) {
            // TODO Auto-generated method stub
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("成功停止 Container " + containerId);
            }
            containers.remove(containerId);
        }

        /**
         * 调用时机:当NodeManager启动Container过程中抛出异常时调用
         */
        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            // TODO Auto-generated method stub
            LOG.log(Level.SEVERE, "启动Container失败:" + containerId);
            containers.remove(containerId);
        }

        /**
         * 调用时机:当NodeManager查询Container状态异常时调用
         */
        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
            // TODO Auto-generated method stub
            LOG.log(Level.SEVERE, "查询Container信息失败 : "
                    + containerId);
        }

        /**
         * 调用时机:当NodeManager停止Container异常时调用
         */
        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            // TODO Auto-generated method stub
            LOG.log(Level.SEVERE, "停止Container失败: " + containerId);
            containers.remove(containerId);
        }

    }

    /**
     * 启动将要执行shell命令的Container的线程类
     *
     * @author niuhy
     */
    private class LaunchContainerRunnable implements Runnable {

        Container container;
        NMCallbackHandler containerListener;

        public LaunchContainerRunnable(Container allocatedContainer, NMCallbackHandler containerListener) {
            // TODO Auto-generated constructor stub
            this.container = allocatedContainer;
            this.containerListener = containerListener;
        }

        @Override
        public void run() {
            // TODO Auto-generated method stub
            String containerId = container.getId().toString();

            LOG.info("配置启动Container,id=" + container.getId());

            ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
//			配置启动Container的环境变量
            ctx.setEnvironment(shellEnv);

//			配置启动Container的相关资源
            Map<String, LocalResource> localResources = new HashMap<>();

            String applicationId = container.getId().getApplicationAttemptId().getApplicationId().toString();

            try {
                FileSystem fs = FileSystem.get(config);

                LocalResource jbossDist = Records.newRecord(LocalResource.class);
                jbossDist.setType(LocalResourceType.ARCHIVE);
                jbossDist.setVisibility(LocalResourceVisibility.APPLICATION);
                Path jbossDistPath = new Path(new URI(JBossConstants.JBOSS_DIST_PATH));
                jbossDist.setResource(ConverterUtils.getYarnUrlFromPath(jbossDistPath));
                FileStatus jbossDistFileStatus = fs.getFileStatus(jbossDistPath);
                jbossDist.setTimestamp(jbossDistFileStatus.getModificationTime());
                jbossDist.setSize(jbossDistFileStatus.getLen());
                localResources.put(JBossConstants.JBOSS_SYMLINK, jbossDist);

                LocalResource jbossConf = Records.newRecord(LocalResource.class);
                jbossConf.setType(LocalResourceType.FILE);
                jbossConf.setVisibility(LocalResourceVisibility.APPLICATION);
                Path jbossConfPath = new Path(new URI(appJar));
                jbossConf.setResource(ConverterUtils.getYarnUrlFromPath(jbossConfPath));
                FileStatus jbossConfPathStatus = fs.getFileStatus(jbossConfPath);
                jbossConf.setTimestamp(jbossConfPathStatus.getModificationTime());
                jbossConf.setSize(jbossConfPathStatus.getLen());
                localResources.put(JBossConstants.JBOSS_ON_YARN_APP, jbossConf);

            } catch (Exception e) {
                // TODO Auto-generated catch block
                LOG.log(Level.SEVERE, "配置本地资源出错！");
                numCompletedContainers.incrementAndGet();
                numFailedContainers.incrementAndGet();
                return;
            }
            ctx.setLocalResources(localResources);

//			配置启动Container的shell命令
            List<String> commands = new ArrayList<>();

            String host = container.getNodeId().getHost();
            String containerHome = config.get("yarn.nodemanager.local-dirs")
                    + File.separator + ContainerLocalizer.USERCACHE
                    + File.separator
                    + System.getenv().get(Environment.USER.toString())
                    + File.separator + ContainerLocalizer.APPCACHE
                    + File.separator + applicationId + File.separator
                    + containerId;
            jbossHome = containerHome + File.separator
                    + JBossConstants.JBOSS_SYMLINK
                    + File.separator + JBossConstants.JBOSS_VERSION;
            String jbossPermissionsCommand = String.format("chmod -R 777 %s", jbossHome);

            int portOffset = 0;
            int containerCount = containerListener.getContainerCount();
            if (containerCount > 1) {
                portOffset = containerCount * 150;
            }

            String domainControllerValue;
            if (domainController == null) {
                domainControllerValue = host;
            } else {
                domainControllerValue = domainController;
            }

            String jbossConfigurationCommand = String
                    .format("%s/bin/java -cp %s %s --home %s --server_group %s --server %s --port_offset %s --admin_user %s --admin_password %s --domain_controller %s --host %s",
                            Environment.JAVA_HOME.$(),
                            "/opt/hadoop-2.6.0/share/hadoop/common/lib/*"
                                    + File.pathSeparator + containerHome
                                    + File.separator
                                    + JBossConstants.JBOSS_ON_YARN_APP,
                            JBossConfiguration.class.getName(), jbossHome,
                            applicationId, containerId, portOffset, adminUser,
                            adminPassword, domainControllerValue, host);

            LOG.info("Configuring JBoss on " + host + " with: "
                    + jbossConfigurationCommand);

            String jbossCommand = String
                    .format("%s%sbin%sdomain.sh -Djboss.bind.address=%s -Djboss.bind.address.management=%s -Djboss.bind.address.unsecure=%s",
                            jbossHome, File.separator, File.separator, host,
                            host, host);

            LOG.info("Starting JBoss with: " + jbossCommand);

            commands.add(jbossPermissionsCommand);
            commands.add(JBossConstants.COMMAND_CHAIN);
            commands.add(jbossConfigurationCommand);
            commands.add(JBossConstants.COMMAND_CHAIN);
            commands.add(jbossCommand);

            ctx.setCommands(commands);

            containerListener.addContainer(container.getId(), container);

            nmClient.startContainerAsync(container, ctx);
        }

    }
}
