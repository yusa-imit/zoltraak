const std = @import("std");

pub fn build(b: *std.Build) void {
    // Target configuration
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Import sailor dependency
    const sailor_dep = b.dependency("sailor", .{
        .target = target,
        .optimize = optimize,
    });
    const sailor_mod = sailor_dep.module("sailor");

    // Executable: zoltraak (server)
    const exe = b.addExecutable(.{
        .name = "zoltraak",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.root_module.addImport("sailor", sailor_mod);

    // Executable: zoltraak-cli (REPL client)
    const cli = b.addExecutable(.{
        .name = "zoltraak-cli",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/cli.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    cli.root_module.addImport("sailor", sailor_mod);

    // Install both executables
    b.installArtifact(exe);
    b.installArtifact(cli);

    // Run step
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());

    // Allow passing arguments to the application
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run zoltraak");
    run_step.dependOn(&run_cmd.step);

    // Test suite - Unit tests
    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    unit_tests.root_module.addImport("sailor", sailor_mod);

    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);

    // Integration tests
    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_integration.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_integration_tests = b.addRunArtifact(integration_tests);
    const integration_test_step = b.step("test-integration", "Run integration tests");
    integration_test_step.dependOn(&run_integration_tests.step);

    // Extended CONFIG command integration tests
    const config_extended_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_config_extended.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_config_extended_tests = b.addRunArtifact(config_extended_tests);
    const config_extended_test_step = b.step("test-config-extended", "Run extended CONFIG integration tests");
    config_extended_test_step.dependOn(&run_config_extended_tests.step);

    // Add extended tests to main integration test step
    integration_test_step.dependOn(&run_config_extended_tests.step);

    // GEOSEARCH BYBOX integration tests
    const geosearch_bybox_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_geosearch_bybox.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_geosearch_bybox_tests = b.addRunArtifact(geosearch_bybox_tests);
    integration_test_step.dependOn(&run_geosearch_bybox_tests.step);

    // Pattern-based Pub/Sub integration tests
    const pattern_pubsub_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_pattern_pubsub.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_pattern_pubsub_tests = b.addRunArtifact(pattern_pubsub_tests);
    integration_test_step.dependOn(&run_pattern_pubsub_tests.step);

    // XREAD/XREADGROUP BLOCK integration tests
    const xread_blocking_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_xread_blocking.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_xread_blocking_tests = b.addRunArtifact(xread_blocking_tests);
    integration_test_step.dependOn(&run_xread_blocking_tests.step);

    // TUI snapshot tests (sailor v1.5.0)
    const tui_snapshot_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_tui_snapshot.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    tui_snapshot_tests.root_module.addImport("sailor", sailor_mod);

    const run_tui_snapshot_tests = b.addRunArtifact(tui_snapshot_tests);
    test_step.dependOn(&run_tui_snapshot_tests.step);

    // CLIENT TRACKING/TRACKINGINFO/CACHING integration tests (Iteration 87)
    const client_tracking_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_client_tracking.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_client_tracking_tests = b.addRunArtifact(client_tracking_tests);
    integration_test_step.dependOn(&run_client_tracking_tests.step);

    // MONITOR command integration tests (Iteration 90)
    const monitor_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_monitor.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_monitor_tests = b.addRunArtifact(monitor_tests);
    integration_test_step.dependOn(&run_monitor_tests.step);

    // Note: integration tests are NOT added to the main test step because they
    // spawn a server binary and require special lifecycle management.
    // Use `zig build test-integration` to run them separately.
}
