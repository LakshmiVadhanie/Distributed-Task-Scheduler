package com.taskscheduler.scheduler;

import com.taskscheduler.proto.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * gRPC server that workers connect to for:
 *   - Registration (Register RPC)
 *   - Periodic heartbeats (SendHeartbeat RPC)
 */
public class SchedulerGrpcServer {

    private static final Logger log = LoggerFactory.getLogger(SchedulerGrpcServer.class);

    private final int            port;
    private final WorkerRegistry workerRegistry;
    private final LeaderElection leaderElection;
    private final String         schedulerId;
    private       Server         server;

    public SchedulerGrpcServer(int port, WorkerRegistry workerRegistry,
                                LeaderElection leaderElection, String schedulerId) {
        this.port           = port;
        this.workerRegistry = workerRegistry;
        this.leaderElection = leaderElection;
        this.schedulerId    = schedulerId;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new HeartbeatServiceImpl())
                .build()
                .start();
        log.info("Scheduler gRPC server listening on port {}", port);
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
            log.info("Scheduler gRPC server stopped");
        }
    }

    // ── HeartbeatService implementation ──────────────────────────────────

    private class HeartbeatServiceImpl extends HeartbeatServiceGrpc.HeartbeatServiceImplBase {

        @Override
        public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
            log.info("Worker registration: id={} addr={}:{} capacity={}",
                    request.getWorkerId(), request.getHost(), request.getPort(), request.getCapacity());

            WorkerInfo info = new WorkerInfo(
                    request.getWorkerId(),
                    request.getHost(),
                    request.getPort(),
                    request.getCapacity()
            );
            workerRegistry.register(info);

            RegisterResponse response = RegisterResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Registered successfully")
                    .setSchedulerId(schedulerId)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void sendHeartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
            workerRegistry.updateHeartbeat(
                    request.getWorkerId(),
                    request.getActiveTasks(),
                    request.getCpuLoad(),
                    request.getMemoryUsage()
            );

            log.debug("Heartbeat from worker {} | active={} cpu={:.1f}%",
                    request.getWorkerId(), request.getActiveTasks(), request.getCpuLoad());

            HeartbeatResponse response = HeartbeatResponse.newBuilder()
                    .setAcknowledged(true)
                    .setIsLeader(leaderElection.isLeader())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
