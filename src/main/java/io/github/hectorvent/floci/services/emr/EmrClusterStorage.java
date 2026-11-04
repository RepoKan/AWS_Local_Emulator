package io.github.hectorvent.floci.services.emr;

import com.github.dockerjava.api.command.InspectVolumeResponse;
import com.github.dockerjava.api.command.ListVolumesResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Network;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.ContainerTeardown;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerExec;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLiveness;
import io.github.hectorvent.floci.core.common.docker.ContainerSpec;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import io.github.hectorvent.floci.services.emrserverless.SparkJobRunner;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Keeps each EMR cluster's storage in Docker volumes, one for its primary node's {@code /home/hadoop} and one for its
 * {@code /mnt}. Both paths belong to the image's {@code hadoop} user, and Docker gives an empty volume what the image
 * has at the path it is first mounted on, owner included, so the steps, which run as that user, can write to both,
 * and what the cluster's bootstrap left there comes with them. Its HDFS, which every EMR cluster runs (from
 * emr-7.14.0 on, "When you select Trino as the only application at launch, Amazon EMR no longer starts YARN and
 * HDFS", a cluster Floci does not run), is a NameNode and a DataNode in a container of their own, the cluster's
 * primary node's, with their data in a third volume, so that the first container to mount the {@code /mnt} one is a
 * step's, which fills it from the step's image.
 */
@ApplicationScoped
public class EmrClusterStorage implements ClusterStorage, ContainerTeardown {

    private static final Logger LOG = Logger.getLogger(EmrClusterStorage.class);
    private static final String SERVICE = "emr";
    private static final String COMPONENT = "emr-storage";
    private static final String HDFS_COMPONENT = "emr-hdfs";
    private static final String HDFS_CONF = "/tmp/floci-hdfs-conf";
    private static final String HDFS_READY = "/tmp/floci-hdfs-ready";
    private static final String HOST_NETWORK = "host";
    private static final String LOOPBACK = "127.0.0.1";
    // The NameNode's JMX on its web port, which says when the DataNode has joined and safe mode has ended without
    // starting a JVM of hdfs's own each time it is asked; each look is given five seconds, so one the NameNode does not
    // answer while it starts is asked again.
    private static final String NAMENODE_JMX = "http://127.0.0.1:9870/jmx?qry=Hadoop:service=NameNode,name=";
    // How long a cluster's HDFS may take to start, its NameNode formatted the first time, before a step gives up.
    private static final Duration HDFS_START_TIMEOUT = Duration.ofSeconds(120);
    private static final Duration HDFS_POLL_INTERVAL = Duration.ofMillis(500);
    // The primary node's HDFS, started as root: its daemons log their warnings and errors to the container's output,
    // which docker logs shows; its host name, FLOCI_HDFS_HOST, leads to this container's address, where the DataNode
    // registers, so steps reach both by it; /mnt, its data's volume, becomes the image's user's, which runs a NameNode
    // and a DataNode with their data in it, the NameNode formatted the first time, with one copy of each block and
    // without permissions, listening on FLOCI_HDFS_BIND: every address of the container, on the cluster's own network,
    // or, on the host's, only its loopback, which its name leads to, so the host's other interfaces do not serve files
    // anyone could change. Once the DataNode has joined and the NameNode has left safe mode, which on a restart waits
    // for the blocks the volume holds, the folders EMR's HDFS has for steps, /tmp and the hadoop user's home, are made,
    // and HDFS_READY says it is ready. A daemon that exits, before or after, ends the container, its readiness gone, so
    // the next step starts it again on its data.
    private static final String HDFS_SCRIPT = String.join("\n",
            "address=$FLOCI_HDFS_BIND; [ \"$address\" != 0.0.0.0 ] || address=$(hostname -i | awk '{print $1}')",
            "echo \"$address $FLOCI_HDFS_HOST\" >> /etc/hosts || exit",
            "chown hadoop:hadoop /mnt || exit",
            "mkdir -p " + HDFS_CONF + " || exit",
            "printf '%s\\n' '<configuration>'"
                    + " \"<property><name>fs.defaultFS</name><value>hdfs://$FLOCI_HDFS_HOST:8020</value></property>\""
                    + " '</configuration>' > " + HDFS_CONF + "/core-site.xml",
            "printf '%s\\n' '<configuration>'"
                    + " '<property><name>dfs.namenode.name.dir</name><value>file:///mnt/namenode</value></property>'"
                    + " '<property><name>dfs.datanode.data.dir</name><value>file:///mnt/datanode</value></property>'"
                    + " '<property><name>dfs.replication</name><value>1</value></property>'"
                    + " '<property><name>dfs.permissions.enabled</name><value>false</value></property>'"
                    + " \"<property><name>dfs.namenode.rpc-bind-host</name><value>$FLOCI_HDFS_BIND</value></property>\""
                    + " \"<property><name>dfs.namenode.http-address</name>"
                    + "<value>$FLOCI_HDFS_BIND:9870</value></property>\""
                    + " \"<property><name>dfs.datanode.address</name><value>$FLOCI_HDFS_BIND:9866</value></property>\""
                    + " \"<property><name>dfs.datanode.http.address</name>"
                    + "<value>$FLOCI_HDFS_BIND:9864</value></property>\""
                    + " \"<property><name>dfs.datanode.ipc.address</name>"
                    + "<value>$FLOCI_HDFS_BIND:9867</value></property>\""
                    + " '</configuration>' > " + HDFS_CONF + "/hdfs-site.xml",
            "printf '%s\\n' 'log4j.rootLogger=WARN,console' 'log4j.appender.console=org.apache.log4j.ConsoleAppender'"
                    + " 'log4j.appender.console.layout=org.apache.log4j.PatternLayout'"
                    + " 'log4j.appender.console.layout.ConversionPattern=%d %p %c: %m%n'"
                    + " > " + HDFS_CONF + "/log4j.properties",
            "exec setpriv --reuid=hadoop --regid=hadoop --clear-groups bash -c '"
                    + "[ -d /mnt/namenode/current ] || hdfs namenode -format -nonInteractive || exit; "
                    + "hdfs namenode & hdfs datanode & "
                    + "until curl -sf -m 5 \"" + NAMENODE_JMX + "FSNamesystemState\""
                    + " | grep -q \"\\\"NumLiveDataNodes\\\" : 1,\"; do "
                    + "[ \"$(jobs -rp | wc -l)\" = 2 ] || exit 1; sleep 0.2; done; "
                    + "until curl -sf -m 5 \"" + NAMENODE_JMX + "NameNodeInfo\""
                    + " | grep -q \"\\\"Safemode\\\" : \\\"\\\"\"; do "
                    + "[ \"$(jobs -rp | wc -l)\" = 2 ] || exit 1; sleep 0.2; done; "
                    + "hdfs dfs -mkdir -p /tmp /user/hadoop || exit 1; "
                    + "touch " + HDFS_READY + "; "
                    + "wait -n; rm -f " + HDFS_READY + "; kill $(jobs -rp) 2>/dev/null; exit 1'");

    // One start at a time for each cluster's HDFS; a step coming while it starts waits, then finds it running.
    private final Map<String, Object> starting = new ConcurrentHashMap<>();
    // The HDFS containers this process has started or found running, by name, which a reset or a shutdown removes.
    private final Set<String> running = ConcurrentHashMap.newKeySet();
    private final ContainerLifecycleManager lifecycleManager;
    private final ContainerBuilder containerBuilder;
    private final EmulatorConfig config;
    private final Duration hdfsStartTimeout;
    private final LongSupplier nanoTime;
    // How many teardowns, by a reset or a shutdown, have run: a step waiting for its cluster's HDFS through one stops.
    private final AtomicLong teardowns = new AtomicLong();

    @Inject
    public EmrClusterStorage(ContainerLifecycleManager lifecycleManager, ContainerBuilder containerBuilder,
                             EmulatorConfig config) {
        this(lifecycleManager, containerBuilder, config, HDFS_START_TIMEOUT, System::nanoTime);
    }

    EmrClusterStorage(ContainerLifecycleManager lifecycleManager, ContainerBuilder containerBuilder,
                      EmulatorConfig config, Duration hdfsStartTimeout, LongSupplier nanoTime) {
        this.lifecycleManager = lifecycleManager;
        this.containerBuilder = containerBuilder;
        this.config = config;
        this.hdfsStartTimeout = hdfsStartTimeout;
        this.nanoTime = nanoTime;
    }

    @Override
    public SparkJobRunner.Storage volumes(String clusterId) {
        return new SparkJobRunner.Storage(
                ContainerStorageHelper.dockerName(config, SERVICE + "-" + clusterId + "-home"),
                ContainerStorageHelper.dockerName(config, SERVICE + "-" + clusterId + "-mnt"));
    }

    @Override
    public void create(String clusterId, String accountId, String region) {
        Map<String, String> labels = labels(clusterId, accountId, region, COMPONENT);
        SparkJobRunner.Storage storage = volumes(clusterId);
        lifecycleManager.ensureVolume(storage.home(), labels);
        lifecycleManager.ensureVolume(storage.mnt(), labels);
    }

    @Override
    public SparkJobRunner.Storage hdfs(String clusterId, String accountId, String region, String host, String image,
                                       String dockerNetwork) {
        String name = hdfsName(clusterId);
        // From when the step asks, so one waiting while another step starts the HDFS is not given the time again.
        long deadline = nanoTime.getAsLong() + hdfsStartTimeout.toNanos();
        long teardown = teardowns.get();
        synchronized (starting.computeIfAbsent(clusterId, id -> new Object())) {
            String containerId = null;
            // One found running whose daemon has just exited stops before it is ready, and is started again, once.
            for (int attempt = 0; containerId == null; attempt++) {
                stopIfTornDown(clusterId, teardown);
                ContainerLiveness liveness = lifecycleManager.probeContainer(name);
                if (liveness == ContainerLiveness.UNKNOWN) {
                    // Docker did not answer: one that may be serving the cluster's other steps is left as it is.
                    throw new IllegalStateException("Could not tell whether the HDFS of EMR cluster " + clusterId
                            + " runs");
                }
                boolean found = liveness == ContainerLiveness.RUNNING && attempt == 0;
                String candidate = found ? name : start(clusterId, accountId, region, host, image, dockerNetwork);
                running.add(name);
                if (awaitReady(clusterId, candidate, deadline, teardown)) {
                    containerId = candidate;
                } else if (!found) {
                    stopIfTornDown(clusterId, teardown);
                    throw new IllegalStateException("The HDFS of EMR cluster " + clusterId
                            + " stopped before it was ready");
                }
            }
            if (onHostNetwork(dockerNetwork)) {
                return volumes(clusterId).withHdfs(host, LOOPBACK, null);
            }
            return volumes(clusterId).withHdfs(host, address(clusterId, containerId), name);
        }
    }

    // Where a step reaches the cluster's HDFS: its container's address on the cluster's own Docker network.
    private String address(String clusterId, String containerId) {
        String address = lifecycleManager.resolveContainerNetworkIp(containerId, hdfsName(clusterId));
        if (address == null || address.isBlank()) {
            throw new IllegalStateException("The HDFS of EMR cluster " + clusterId
                    + " has no address on Docker network " + hdfsName(clusterId));
        }
        return address;
    }

    // Whether steps run on the host's own network, docker-network's, else the global one's, which the HDFS shares.
    private boolean onHostNetwork(String dockerNetwork) {
        return HOST_NETWORK.equals(containerBuilder.resolveDockerNetwork(Optional.ofNullable(dockerNetwork))
                .orElse(null));
    }

    // A cluster's HDFS container, started anew on the HDFS data its volume holds, which goes with the cluster's other
    // two; one a restart, a stop or a daemon's exit left goes first. It runs on a Docker network of the cluster's own,
    // which only the cluster's steps join, as its HDFS serves its files to any client that reaches it: the steps of
    // other clusters, of other accounts too, do not. On the host's own network it listens on the loopback alone.
    private String start(String clusterId, String accountId, String region, String host, String image,
                         String dockerNetwork) {
        String name = hdfsName(clusterId);
        boolean hostNetwork = onHostNetwork(dockerNetwork);
        lifecycleManager.removeIfExists(name);
        lifecycleManager.ensureVolume(name, labels(clusterId, accountId, region, COMPONENT));
        if (!hostNetwork) {
            lifecycleManager.ensureNetwork(name, labels(clusterId, accountId, region, HDFS_COMPONENT));
        }
        ContainerSpec spec = containerBuilder.newContainer(image)
                .withName(name)
                .withUser("root")
                .withEntrypoint(List.of("/bin/bash", "-c", HDFS_SCRIPT))
                .withEnv("FLOCI_HDFS_HOST", host)
                .withEnv("FLOCI_HDFS_BIND", hostNetwork ? LOOPBACK : "0.0.0.0")
                .withEnv("HADOOP_CONF_DIR", HDFS_CONF)
                .withEnv("HADOOP_LOG_DIR", "/tmp/floci-hdfs-logs")
                .withNamedVolume(name, "/mnt")
                .withDockerNetwork(Optional.of(hostNetwork ? HOST_NETWORK : name))
                .withLogRotation()
                .withLabels(labels(clusterId, accountId, region, HDFS_COMPONENT))
                .build();
        String containerId = lifecycleManager.createAndStart(spec).containerId();
        LOG.infov("Started the HDFS of EMR cluster {0} as {1}", clusterId, host);
        return containerId;
    }

    // Whether the container became ready by the deadline, false when it stopped first or a teardown has run since the
    // step asked; a failure to become ready in time is thrown.
    private boolean awaitReady(String clusterId, String containerId, long deadline, long teardown) {
        while (teardowns.get() == teardown) {
            // Docker refuses an exec into a container that has stopped, so that is looked at first.
            if (lifecycleManager.probeContainer(containerId) == ContainerLiveness.NOT_RUNNING) {
                return false;
            }
            long exitCode;
            try {
                exitCode = ContainerExec.run(lifecycleManager.getDockerClient(), containerId,
                        new String[]{"test", "-f", HDFS_READY}, 10).exitCode();
            } catch (RuntimeException e) {
                // It may have stopped after it was looked at, which makes Docker refuse the exec.
                if (lifecycleManager.probeContainer(containerId) == ContainerLiveness.NOT_RUNNING) {
                    return false;
                }
                throw e;
            }
            if (exitCode == 0) {
                return true;
            }
            if (nanoTime.getAsLong() >= deadline) {
                throw new IllegalStateException("The HDFS of EMR cluster " + clusterId + " did not start within "
                        + hdfsStartTimeout.toSeconds() + " seconds");
            }
            try {
                Thread.sleep(HDFS_POLL_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Stopped waiting for the HDFS of EMR cluster " + clusterId, e);
            }
        }
        return false;
    }

    // A teardown since the step asked for its cluster's HDFS stops the step as it would its driver: the container
    // goes, one the step started after the teardown looked too.
    private void stopIfTornDown(String clusterId, long teardown) {
        if (teardowns.get() != teardown) {
            running.remove(hdfsName(clusterId));
            lifecycleManager.removeIfExists(hdfsName(clusterId));
            throw new ClusterStorage.Stopped("Floci stopped the HDFS of EMR cluster " + clusterId);
        }
    }

    // The name of a cluster's HDFS container, of the volume its data is in, and of the Docker network it is on.
    private String hdfsName(String clusterId) {
        return ContainerStorageHelper.dockerName(config, SERVICE + "-" + clusterId + "-hdfs");
    }

    private Map<String, String> labels(String clusterId, String accountId, String region, String component) {
        Map<String, String> labels = new LinkedHashMap<>(ContainerStorageHelper.resourceIdentityLabels(SERVICE,
                clusterId, accountId, region));
        labels.put(ContainerStorageHelper.COMPONENT_LABEL, component);
        labels.put(ContainerStorageHelper.OWNER_LABEL, ContainerStorageHelper.ownerIdentity(config));
        return labels;
    }

    @Override
    public boolean remove(String clusterId) {
        // Its HDFS goes first, as its container holds the volume of its data and its network, and its lock with it, so
        // the clusters that have ended leave none behind.
        starting.remove(clusterId);
        running.remove(hdfsName(clusterId));
        lifecycleManager.removeIfExists(hdfsName(clusterId));
        SparkJobRunner.Storage storage = volumes(clusterId);
        // Each is tried, so one Docker refused does not keep the others.
        boolean networkGone = lifecycleManager.removeNetwork(hdfsName(clusterId));
        boolean homeGone = lifecycleManager.removeVolume(storage.home());
        boolean mntGone = lifecycleManager.removeVolume(storage.mnt());
        return lifecycleManager.removeVolume(hdfsName(clusterId)) && mntGone && homeGone && networkGone;
    }

    /**
     * Removes the HDFS containers this process has started or found running, so a reset or a shutdown leaves none
     * running; a cluster's next step starts its HDFS again, on the data its volume keeps. A step waiting for one
     * stops, as its driver would, and removes one it started since. This runs after the teardowns of the drivers,
     * which use the HDFS, so theirs stop first.
     */
    @Override
    public void stopManagedContainers() {
        teardowns.incrementAndGet();
        for (String name : List.copyOf(running)) {
            running.remove(name);
            lifecycleManager.removeIfExists(name);
        }
    }

    @Override
    public int teardownOrder() {
        return 1;
    }

    @Override
    public void removeAllBut(Set<String> kept) {
        try {
            for (Container container : lifecycleManager.getDockerClient().listContainersCmd().withShowAll(true)
                    .withLabelFilter(Map.of(ContainerStorageHelper.COMPONENT_LABEL, HDFS_COMPONENT,
                            ContainerStorageHelper.OWNER_LABEL, ContainerStorageHelper.ownerIdentity(config)))
                    .exec()) {
                Map<String, String> labels = container.getLabels() != null ? container.getLabels() : Map.of();
                if (!kept.contains(labels.get(ContainerStorageHelper.RESOURCE_ID_LABEL))) {
                    lifecycleManager.removeIfExists(container.getId());
                }
            }
            for (Network network : lifecycleManager.getDockerClient().listNetworksCmd()
                    .withFilter("label", List.of(ContainerStorageHelper.COMPONENT_LABEL + "=" + HDFS_COMPONENT,
                            ContainerStorageHelper.OWNER_LABEL + "=" + ContainerStorageHelper.ownerIdentity(config)))
                    .exec()) {
                Map<String, String> labels = network.getLabels() != null ? network.getLabels() : Map.of();
                if (!kept.contains(labels.get(ContainerStorageHelper.RESOURCE_ID_LABEL))) {
                    lifecycleManager.removeNetwork(network.getName());
                }
            }
            ListVolumesResponse response = lifecycleManager.getDockerClient().listVolumesCmd()
                    .withFilter("label", List.of(ContainerStorageHelper.COMPONENT_LABEL + "=" + COMPONENT,
                            ContainerStorageHelper.OWNER_LABEL + "=" + ContainerStorageHelper.ownerIdentity(config)))
                    .exec();
            List<InspectVolumeResponse> volumes = response != null && response.getVolumes() != null
                    ? response.getVolumes() : List.of();
            for (InspectVolumeResponse volume : volumes) {
                Map<String, String> labels = volume.getLabels() != null ? volume.getLabels() : Map.of();
                if (!kept.contains(labels.get(ContainerStorageHelper.RESOURCE_ID_LABEL))) {
                    lifecycleManager.removeVolume(volume.getName());
                }
            }
        } catch (RuntimeException e) {
            LOG.warnv("Could not remove the storage of EMR clusters that have ended: {0}", e.getMessage());
        }
    }
}
