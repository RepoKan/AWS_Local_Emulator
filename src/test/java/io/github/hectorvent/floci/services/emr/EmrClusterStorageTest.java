package io.github.hectorvent.floci.services.emr;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmd;
import com.github.dockerjava.api.command.ListContainersCmd;
import com.github.dockerjava.api.command.ListNetworksCmd;
import com.github.dockerjava.api.command.ListVolumesCmd;
import com.github.dockerjava.api.command.ListVolumesResponse;
import com.github.dockerjava.api.exception.ConflictException;
import com.github.dockerjava.api.exception.DockerException;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Network;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.ContainerTeardown;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerExecStubs;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLiveness;
import io.github.hectorvent.floci.core.common.docker.ContainerSpec;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import io.github.hectorvent.floci.services.emrserverless.SparkJobRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** A cluster's storage: Docker volumes, and its HDFS in a container of its own, on its primary node. */
class EmrClusterStorageTest {

    private static final String IMAGE = "public.ecr.aws/emr-serverless/hive/emr-7.5.0:latest";
    private static final String HOST = "ip-10-0-0-1.ec2.internal";
    private static final String READY = "test -f /tmp/floci-hdfs-ready";

    private final EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
    private final DockerClient docker = mock(DockerClient.class);
    private final ContainerLifecycleManager lifecycle = mock(ContainerLifecycleManager.class);
    private final ContainerBuilder containerBuilder = mock(ContainerBuilder.class);
    private final ContainerBuilder.Builder spec = mock(ContainerBuilder.Builder.class, RETURNS_SELF);
    private final EmrClusterStorage storage = new EmrClusterStorage(lifecycle, containerBuilder, config);
    private final String hdfsName = ContainerStorageHelper.dockerName(config, "emr-j-1-hdfs");

    @BeforeEach
    void docker() {
        when(lifecycle.getDockerClient()).thenReturn(docker);
        when(containerBuilder.resolveDockerNetwork(Optional.of("floci-network")))
                .thenReturn(Optional.of("floci-network"));
        when(containerBuilder.newContainer(IMAGE)).thenReturn(spec);
        when(spec.build()).thenReturn(new ContainerSpec(IMAGE));
    }

    @Test
    void aClustersHdfsStartsAsRootFromTheReleasesImageOnAVolumeOfItsOwnAndAnswersAtItsAddressOnceReady() {
        when(lifecycle.probeContainer(hdfsName)).thenReturn(ContainerLiveness.NOT_RUNNING);
        when(lifecycle.createAndStart(any())).thenReturn(new ContainerLifecycleManager.ContainerInfo("c-1", Map.of()));
        when(lifecycle.probeContainer("c-1")).thenReturn(ContainerLiveness.RUNNING);
        when(lifecycle.resolveContainerNetworkIp("c-1", hdfsName)).thenReturn("172.18.0.5");
        // Not ready the first time it is looked at, then ready.
        List<List<String>> looks = ContainerExecStubs.completeEveryExec(docker, "c-1", List.of(1L, 0L), "", "");

        SparkJobRunner.Storage started = storage.hdfs("j-1", "123456789012", "us-east-1", HOST, IMAGE,
                "floci-network");

        SparkJobRunner.Storage volumes = storage.volumes("j-1");
        assertEquals(new SparkJobRunner.Storage(volumes.home(), volumes.mnt(), HOST, "172.18.0.5", hdfsName), started);
        verify(spec).withName(hdfsName);
        verify(spec).withUser("root");
        verify(spec).withEnv("FLOCI_HDFS_HOST", HOST);
        verify(spec).withEnv("FLOCI_HDFS_BIND", "0.0.0.0");
        // Not the cluster's /mnt volume, which Docker fills from the image of the first container to mount it: a
        // step's, so what the bootstrap left there comes with it.
        verify(spec).withNamedVolume(hdfsName, "/mnt");
        verify(lifecycle).ensureVolume(eq(hdfsName), argThat(labels ->
                "emr-storage".equals(labels.get(ContainerStorageHelper.COMPONENT_LABEL))
                        && "j-1".equals(labels.get(ContainerStorageHelper.RESOURCE_ID_LABEL))));
        // On a network of the cluster's own, which only its steps join, not the one they run on, where other
        // clusters' steps would reach it.
        verify(lifecycle).ensureNetwork(eq(hdfsName), argThat(labels ->
                "emr-hdfs".equals(labels.get(ContainerStorageHelper.COMPONENT_LABEL))
                        && "j-1".equals(labels.get(ContainerStorageHelper.RESOURCE_ID_LABEL))));
        verify(spec).withDockerNetwork(Optional.of(hdfsName));
        verify(spec).withLabels(argThat(labels -> "emr-hdfs".equals(labels.get(ContainerStorageHelper.COMPONENT_LABEL))
                && "j-1".equals(labels.get(ContainerStorageHelper.RESOURCE_ID_LABEL))));
        // Its host name leads to the container's own address, where the DataNode registers, unless it listens on one
        // address only, which its name then leads to.
        verify(spec).withEntrypoint(argThat(entrypoint -> entrypoint.get(2).startsWith(String.join("\n",
                "address=$FLOCI_HDFS_BIND; [ \"$address\" != 0.0.0.0 ] || address=$(hostname -i | awk '{print $1}')",
                "echo \"$address $FLOCI_HDFS_HOST\" >> /etc/hosts || exit\n"))));
        // Every one of its daemons' listeners is on that address.
        verify(spec).withEntrypoint(argThat(entrypoint -> entrypoint.get(2).contains(String.join(" ",
                "\"<property><name>dfs.namenode.rpc-bind-host</name><value>$FLOCI_HDFS_BIND</value></property>\"",
                "\"<property><name>dfs.namenode.http-address</name><value>$FLOCI_HDFS_BIND:9870</value></property>\"",
                "\"<property><name>dfs.datanode.address</name><value>$FLOCI_HDFS_BIND:9866</value></property>\"",
                "\"<property><name>dfs.datanode.http.address</name><value>$FLOCI_HDFS_BIND:9864</value></property>\"",
                "\"<property><name>dfs.datanode.ipc.address</name>"
                        + "<value>$FLOCI_HDFS_BIND:9867</value></property>\""))));
        assertEquals(List.of(List.of(READY.split(" ")), List.of(READY.split(" "))), looks);
        // Its daemons' warnings and errors in the container's output.
        verify(spec).withEntrypoint(argThat(entrypoint -> entrypoint.get(2).contains(
                "printf '%s\\n' 'log4j.rootLogger=WARN,console'"
                        + " 'log4j.appender.console=org.apache.log4j.ConsoleAppender'")));
        // Ready once the DataNode has joined and the NameNode has left safe mode, as its JMX says; a daemon that
        // exits ends it.
        verify(spec).withEntrypoint(argThat(entrypoint -> entrypoint.get(2).contains(String.join(" ",
                "until curl -sf -m 5 \"http://127.0.0.1:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState\"",
                "| grep -q \"\\\"NumLiveDataNodes\\\" : 1,\"; do",
                "[ \"$(jobs -rp | wc -l)\" = 2 ] || exit 1; sleep 0.2; done;",
                "until curl -sf -m 5 \"http://127.0.0.1:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo\"",
                "| grep -q \"\\\"Safemode\\\" : \\\"\\\"\"; do",
                "[ \"$(jobs -rp | wc -l)\" = 2 ] || exit 1; sleep 0.2; done;",
                "hdfs dfs -mkdir -p /tmp /user/hadoop || exit 1;",
                "touch /tmp/floci-hdfs-ready;",
                "wait -n; rm -f /tmp/floci-hdfs-ready; kill $(jobs -rp) 2>/dev/null; exit 1'"))));
        // One left stopped by a restart goes first.
        InOrder order = inOrder(lifecycle);
        order.verify(lifecycle).removeIfExists(hdfsName);
        order.verify(lifecycle).ensureVolume(eq(hdfsName), any());
        order.verify(lifecycle).ensureNetwork(eq(hdfsName), any());
        order.verify(lifecycle).createAndStart(any());
    }

    @Test
    void onTheHostsNetworkAStepReachesItsClustersHdfsAtTheHostsLoopback() {
        when(containerBuilder.resolveDockerNetwork(Optional.of("host"))).thenReturn(Optional.of("host"));
        when(lifecycle.probeContainer(hdfsName)).thenReturn(ContainerLiveness.RUNNING);
        ContainerExecStubs.completeEveryExec(docker, hdfsName, 0L, "", "");

        SparkJobRunner.Storage started = storage.hdfs("j-1", "123456789012", "us-east-1", HOST, IMAGE, "host");

        assertEquals("127.0.0.1", started.hdfsAddress());
        // Steps share the host's network with it, so they join no other.
        assertNull(started.hdfsNetwork());
        verify(lifecycle, never()).resolveContainerNetworkIp(anyString(), any());
    }

    @Test
    void onTheHostsNetworkAClustersHdfsListensOnlyOnTheHostsLoopback() {
        when(containerBuilder.resolveDockerNetwork(Optional.of("host"))).thenReturn(Optional.of("host"));
        when(lifecycle.probeContainer(hdfsName)).thenReturn(ContainerLiveness.NOT_RUNNING);
        when(lifecycle.createAndStart(any())).thenReturn(new ContainerLifecycleManager.ContainerInfo("c-1", Map.of()));
        when(lifecycle.probeContainer("c-1")).thenReturn(ContainerLiveness.RUNNING);
        ContainerExecStubs.completeEveryExec(docker, "c-1", 0L, "", "");

        storage.hdfs("j-1", "123456789012", "us-east-1", HOST, IMAGE, "host");

        // The host's other interfaces would otherwise serve its files, without permissions, to anyone reaching them.
        verify(spec).withEnv("FLOCI_HDFS_BIND", "127.0.0.1");
        verify(spec).withDockerNetwork(Optional.of("host"));
        verify(lifecycle, never()).ensureNetwork(anyString(), any());
    }

    @Test
    void aClustersHdfsWithNoAddressOnItsNetworkFailsTheStep() {
        when(lifecycle.probeContainer(hdfsName)).thenReturn(ContainerLiveness.RUNNING);
        when(lifecycle.resolveContainerNetworkIp(hdfsName, hdfsName)).thenReturn("");
        ContainerExecStubs.completeEveryExec(docker, hdfsName, 0L, "", "");

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> storage.hdfs("j-1", "123456789012", "us-east-1", HOST, IMAGE, "floci-network"));

        assertEquals("The HDFS of EMR cluster j-1 has no address on Docker network " + hdfsName,
                failure.getMessage());
    }

    @Test
    void aClustersHdfsThatRunsIsUsedAsItIs() {
        when(lifecycle.probeContainer(hdfsName)).thenReturn(ContainerLiveness.RUNNING);
        when(lifecycle.resolveContainerNetworkIp(hdfsName, hdfsName)).thenReturn("172.18.0.6");
        ContainerExecStubs.completeEveryExec(docker, hdfsName, 0L, "", "");

        assertEquals("172.18.0.6", storage.hdfs("j-1", "123456789012", "us-east-1", HOST, IMAGE, "floci-network")
                .hdfsAddress());

        verify(lifecycle, never()).createAndStart(any());
        verify(lifecycle, never()).removeIfExists(anyString());
    }

    @Test
    void aClustersHdfsThatDockerCannotTellTheStateOfIsLeftAsItIs() {
        when(lifecycle.probeContainer(hdfsName)).thenReturn(ContainerLiveness.UNKNOWN);

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> storage.hdfs("j-1", "123456789012", "us-east-1", HOST, IMAGE, "floci-network"));

        assertEquals("Could not tell whether the HDFS of EMR cluster j-1 runs", failure.getMessage());
        verify(lifecycle, never()).removeIfExists(anyString());
        verify(lifecycle, never()).createAndStart(any());
    }

    @Test
    void aClustersHdfsFoundRunningThatStopsBeforeItIsReadyIsStartedAgainOnItsData() {
        // Found running, then stopped, as a container whose daemon has just exited is.
        when(lifecycle.probeContainer(hdfsName)).thenReturn(ContainerLiveness.RUNNING, ContainerLiveness.NOT_RUNNING);
        when(lifecycle.createAndStart(any())).thenReturn(new ContainerLifecycleManager.ContainerInfo("c-7", Map.of()));
        when(lifecycle.probeContainer("c-7")).thenReturn(ContainerLiveness.RUNNING);
        when(lifecycle.resolveContainerNetworkIp("c-7", hdfsName)).thenReturn("172.18.0.8");
        ContainerExecStubs.completeEveryExec(docker, "c-7", 0L, "", "");

        assertEquals("172.18.0.8", storage.hdfs("j-1", "123456789012", "us-east-1", HOST, IMAGE, "floci-network")
                .hdfsAddress());

        InOrder order = inOrder(lifecycle);
        order.verify(lifecycle).removeIfExists(hdfsName);
        order.verify(lifecycle).createAndStart(any());
        verify(spec).withNamedVolume(hdfsName, "/mnt");
    }

    @Test
    void aClustersHdfsThatStopsBeforeItIsReadyFailsWithoutAnExecDockerWouldRefuse() {
        when(lifecycle.probeContainer(hdfsName)).thenReturn(ContainerLiveness.NOT_RUNNING);
        when(lifecycle.createAndStart(any())).thenReturn(new ContainerLifecycleManager.ContainerInfo("c-3", Map.of()));
        when(lifecycle.probeContainer("c-3")).thenReturn(ContainerLiveness.NOT_RUNNING);
        List<List<String>> looks = ContainerExecStubs.completeEveryExec(docker, "c-3", 1L, "", "");

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> storage.hdfs("j-1", "123456789012", "us-east-1", HOST, IMAGE, "floci-network"));

        assertEquals("The HDFS of EMR cluster j-1 stopped before it was ready", failure.getMessage());
        assertEquals(List.of(), looks);
    }

    @Test
    void aClustersHdfsThatIsNotReadyInTimeFailsToStart() {
        EmrClusterStorage impatient = new EmrClusterStorage(lifecycle, containerBuilder, config, Duration.ZERO,
                System::nanoTime);
        when(lifecycle.probeContainer(hdfsName)).thenReturn(ContainerLiveness.NOT_RUNNING);
        when(lifecycle.createAndStart(any())).thenReturn(new ContainerLifecycleManager.ContainerInfo("c-4", Map.of()));
        when(lifecycle.probeContainer("c-4")).thenReturn(ContainerLiveness.RUNNING);
        ContainerExecStubs.completeEveryExec(docker, "c-4", 1L, "", "");

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> impatient.hdfs("j-1", "123456789012", "us-east-1", HOST, IMAGE, "floci-network"));

        assertEquals("The HDFS of EMR cluster j-1 did not start within 0 seconds", failure.getMessage());
    }

    @Test
    void aStepWaitingWhileAnotherStartsItsClustersHdfsHasOnlyWhatIsLeftOfTheStartTimeout() throws Exception {
        AtomicLong clock = new AtomicLong();
        long timeout = Duration.ofMinutes(2).toNanos();
        EmrClusterStorage timed = new EmrClusterStorage(lifecycle, containerBuilder, config, Duration.ofMinutes(2),
                clock::get);
        // The first step starts it; the second, which waited for it, finds it running.
        when(lifecycle.probeContainer(hdfsName)).thenReturn(ContainerLiveness.NOT_RUNNING, ContainerLiveness.RUNNING);
        when(lifecycle.createAndStart(any())).thenReturn(new ContainerLifecycleManager.ContainerInfo("c-6", Map.of()));
        when(lifecycle.probeContainer("c-6")).thenReturn(ContainerLiveness.RUNNING);
        ContainerExecStubs.completeEveryExec(docker, "c-6", 1L, "", "");
        List<List<String>> secondLooks = ContainerExecStubs.completeEveryExec(docker, hdfsName, 1L, "", "");
        CountDownLatch firstLooks = new CountDownLatch(1);
        AtomicReference<Thread> second = new AtomicReference<>();
        // The first step's look lasts until the second waits for it, and past the timeout.
        onEachExec("c-6", () -> {
            firstLooks.countDown();
            awaitBlocked(second);
            clock.addAndGet(timeout + 1);
        });
        // Each of the second's looks takes a minute, which would leave it time for another from a deadline of its own.
        onEachExec(hdfsName, () -> clock.addAndGet(Duration.ofMinutes(1).toNanos()));
        ExecutorService steps = Executors.newFixedThreadPool(2);
        try {
            Future<SparkJobRunner.Storage> first = steps.submit(
                    () -> timed.hdfs("j-1", "123456789012", "us-east-1", HOST, IMAGE, "floci-network"));
            assertTrue(firstLooks.await(10, TimeUnit.SECONDS));
            Future<SparkJobRunner.Storage> waiting = steps.submit(() -> {
                second.set(Thread.currentThread());
                return timed.hdfs("j-1", "123456789012", "us-east-1", HOST, IMAGE, "floci-network");
            });

            assertThrows(ExecutionException.class, () -> first.get(10, TimeUnit.SECONDS));
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> waiting.get(10, TimeUnit.SECONDS));

            assertEquals("The HDFS of EMR cluster j-1 did not start within 120 seconds",
                    failure.getCause().getMessage());
            assertEquals(1, secondLooks.size());
        } finally {
            steps.shutdownNow();
        }
    }

    @Test
    void aStepWaitingForItsClustersHdfsThroughATeardownStopsAsItsDriverWouldAndStartsItNoMore() {
        AtomicLong clock = new AtomicLong();
        EmrClusterStorage timed = new EmrClusterStorage(lifecycle, containerBuilder, config, Duration.ofMinutes(2),
                clock::get);
        when(lifecycle.probeContainer(hdfsName)).thenReturn(ContainerLiveness.NOT_RUNNING);
        when(lifecycle.createAndStart(any())).thenReturn(new ContainerLifecycleManager.ContainerInfo("c-5", Map.of()));
        when(lifecycle.probeContainer("c-5")).thenReturn(ContainerLiveness.RUNNING);
        ContainerExecStubs.completeEveryExec(docker, "c-5", 1L, "", "");
        // A reset or a shutdown tears it down while it is being looked at; each look takes a minute.
        onEachExec("c-5", () -> {
            timed.stopManagedContainers();
            clock.addAndGet(Duration.ofMinutes(1).toNanos());
        });

        assertThrows(ClusterStorage.Stopped.class,
                () -> timed.hdfs("j-1", "123456789012", "us-east-1", HOST, IMAGE, "floci-network"));

        verify(lifecycle, times(1)).createAndStart(any());
        // As it starts, one left from before goes; then the teardown removes it, and the step again, in case it
        // started it after the teardown looked.
        verify(lifecycle, times(3)).removeIfExists(hdfsName);
    }

    @Test
    void aClustersHdfsThatStopsWhileItsReadinessIsLookedAtIsStartedAgainOnItsData() {
        // Found running, still running when looked at, then stopped, so Docker refuses the exec.
        when(lifecycle.probeContainer(hdfsName)).thenReturn(ContainerLiveness.RUNNING, ContainerLiveness.RUNNING,
                ContainerLiveness.NOT_RUNNING);
        when(docker.execCreateCmd(hdfsName)).thenThrow(new ConflictException("Container is not running"));
        when(lifecycle.createAndStart(any())).thenReturn(new ContainerLifecycleManager.ContainerInfo("c-9", Map.of()));
        when(lifecycle.probeContainer("c-9")).thenReturn(ContainerLiveness.RUNNING);
        when(lifecycle.resolveContainerNetworkIp("c-9", hdfsName)).thenReturn("172.18.0.10");
        ContainerExecStubs.completeEveryExec(docker, "c-9", 0L, "", "");

        assertEquals("172.18.0.10", storage.hdfs("j-1", "123456789012", "us-east-1", HOST, IMAGE, "floci-network")
                .hdfsAddress());

        verify(lifecycle).createAndStart(any());
    }

    @Test
    void aClustersHdfsWhoseReadinessDockerCannotLookAtWhileItRunsFailsTheStepAndIsLeftAsItIs() {
        when(lifecycle.probeContainer(hdfsName)).thenReturn(ContainerLiveness.RUNNING);
        when(docker.execCreateCmd(hdfsName)).thenThrow(new DockerException("The daemon is busy", 500));

        assertThrows(DockerException.class,
                () -> storage.hdfs("j-1", "123456789012", "us-east-1", HOST, IMAGE, "floci-network"));

        verify(lifecycle, never()).createAndStart(any());
        verify(lifecycle, never()).removeIfExists(anyString());
    }

    @Test
    void aClustersHdfsIsTornDownAfterTheDriversThatUseIt() {
        ContainerTeardown drivers = () -> { };

        assertTrue(storage.teardownOrder() > drivers.teardownOrder());
    }

    @Test
    void aClustersStorageGoesWithItsHdfsFirstAsItHoldsTheVolumeOfItsData() {
        SparkJobRunner.Storage volumes = storage.volumes("j-1");
        when(lifecycle.removeVolume(anyString())).thenReturn(true);
        when(lifecycle.removeNetwork(hdfsName)).thenReturn(true);

        assertTrue(storage.remove("j-1"));

        InOrder order = inOrder(lifecycle);
        order.verify(lifecycle).removeIfExists(hdfsName);
        order.verify(lifecycle).removeNetwork(hdfsName);
        order.verify(lifecycle).removeVolume(hdfsName);
        verify(lifecycle).removeVolume(volumes.home());
        verify(lifecycle).removeVolume(volumes.mnt());
    }

    @Test
    void aClustersStorageThatDockerKeepsAnyOfIsNotGone() {
        when(lifecycle.removeVolume(anyString())).thenReturn(true);
        when(lifecycle.removeNetwork(hdfsName)).thenReturn(true);
        when(lifecycle.removeVolume(hdfsName)).thenReturn(false);

        assertFalse(storage.remove("j-1"));

        // Nor is one whose network Docker keeps.
        when(lifecycle.removeVolume(hdfsName)).thenReturn(true);
        when(lifecycle.removeNetwork(hdfsName)).thenReturn(false);

        assertFalse(storage.remove("j-1"));
    }

    @Test
    void aResetOrAShutdownRemovesTheHdfsItFoundRunningAndKeepsTheVolumeOfItsData() {
        when(lifecycle.probeContainer(hdfsName)).thenReturn(ContainerLiveness.RUNNING);
        when(lifecycle.resolveContainerNetworkIp(hdfsName, hdfsName)).thenReturn("172.18.0.9");
        ContainerExecStubs.completeEveryExec(docker, hdfsName, 0L, "", "");
        storage.hdfs("j-1", "123456789012", "us-east-1", HOST, IMAGE, "floci-network");

        storage.stopManagedContainers();
        storage.stopManagedContainers();

        verify(lifecycle).removeIfExists(hdfsName);
        verify(lifecycle, never()).removeVolume(anyString());
        verify(lifecycle, never()).removeNetwork(anyString());
    }

    @Test
    void aResetOrAShutdownAsksDockerNothingWhenNoClustersHdfsRuns() {
        storage.stopManagedContainers();

        verify(lifecycle, never()).getDockerClient();
        verify(lifecycle, never()).removeIfExists(anyString());
    }

    @Test
    void theHdfsOfClustersThatAreGoneOrHaveEndedGoesWithTheirStorage() {
        ListContainersCmd list = mock(ListContainersCmd.class, RETURNS_SELF);
        when(docker.listContainersCmd()).thenReturn(list);
        Container kept = hdfsContainer("c-kept", "j-up");
        Container gone = hdfsContainer("c-gone", "j-ended");
        when(list.exec()).thenReturn(List.of(kept, gone));
        ListNetworksCmd networks = mock(ListNetworksCmd.class, RETURNS_SELF);
        when(docker.listNetworksCmd()).thenReturn(networks);
        Network keptNetwork = hdfsNetwork("emr-j-up-hdfs", "j-up");
        Network goneNetwork = hdfsNetwork("emr-j-ended-hdfs", "j-ended");
        when(networks.exec()).thenReturn(List.of(keptNetwork, goneNetwork));
        ListVolumesCmd volumes = mock(ListVolumesCmd.class, RETURNS_SELF);
        when(docker.listVolumesCmd()).thenReturn(volumes);
        when(volumes.exec()).thenReturn(mock(ListVolumesResponse.class));

        storage.removeAllBut(Set.of("j-up"));

        verify(list).withLabelFilter(argThat((Map<String, String> filter) ->
                "emr-hdfs".equals(filter.get(ContainerStorageHelper.COMPONENT_LABEL))));
        verify(lifecycle).removeIfExists("c-gone");
        verify(lifecycle, never()).removeIfExists("c-kept");
        verify(networks).withFilter(eq("label"), argThat((List<String> filter) ->
                filter.contains(ContainerStorageHelper.COMPONENT_LABEL + "=emr-hdfs")));
        verify(lifecycle).removeNetwork("emr-j-ended-hdfs");
        verify(lifecycle, never()).removeNetwork("emr-j-up-hdfs");
    }

    // Runs hook as each exec into containerId is made, which then answers as its stubs say.
    private void onEachExec(String containerId, Runnable hook) {
        ExecCreateCmd stubbed = docker.execCreateCmd(containerId);
        when(docker.execCreateCmd(containerId)).thenAnswer(invocation -> {
            hook.run();
            return stubbed;
        });
    }

    // Waits until the thread of thread waits for a lock another holds.
    private static void awaitBlocked(AtomicReference<Thread> thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (thread.get() == null || thread.get().getState() != Thread.State.BLOCKED) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("The second step never waited for the first");
            }
            Thread.onSpinWait();
        }
    }

    private static Network hdfsNetwork(String name, String clusterId) {
        Network network = mock(Network.class);
        when(network.getName()).thenReturn(name);
        when(network.getLabels()).thenReturn(Map.of(ContainerStorageHelper.RESOURCE_ID_LABEL, clusterId));
        return network;
    }

    private static Container hdfsContainer(String id, String clusterId) {
        Container container = mock(Container.class);
        when(container.getId()).thenReturn(id);
        when(container.getLabels()).thenReturn(Map.of(ContainerStorageHelper.RESOURCE_ID_LABEL, clusterId));
        return container;
    }
}
