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

    // Import zuda dependency
    const zuda_dep = b.dependency("zuda", .{
        .target = target,
        .optimize = optimize,
    });
    const zuda_mod = zuda_dep.module("zuda");

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
    exe.root_module.addImport("zuda", zuda_mod);

    // Link LuaJIT for Lua scripting support
    exe.linkSystemLibrary("luajit-5.1");
    exe.linkLibC();
    exe.addIncludePath(.{ .cwd_relative = "/opt/homebrew/opt/luajit/include/luajit-2.1" });
    exe.addLibraryPath(.{ .cwd_relative = "/opt/homebrew/opt/luajit/lib" });

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

    // Link LuaJIT for Lua scripting tests
    unit_tests.linkSystemLibrary("luajit-5.1");
    unit_tests.linkLibC();
    unit_tests.addIncludePath(.{ .cwd_relative = "/opt/homebrew/opt/luajit/include/luajit-2.1" });
    unit_tests.addLibraryPath(.{ .cwd_relative = "/opt/homebrew/opt/luajit/lib" });

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

    // LATENCY command integration tests (Iteration 92)
    const latency_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_latency.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_latency_tests = b.addRunArtifact(latency_tests);
    integration_test_step.dependOn(&run_latency_tests.step);

    // FAILOVER command integration tests (Iteration 97)
    const failover_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_failover.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_failover_tests = b.addRunArtifact(failover_tests);
    integration_test_step.dependOn(&run_failover_tests.step);

    // MEMORY command integration tests (Iteration 93)
    const memory_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_memory.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_memory_tests = b.addRunArtifact(memory_tests);
    integration_test_step.dependOn(&run_memory_tests.step);

    // Sailor v1.14.0 feature tests (Iteration 103)
    const sailor_v1_14_0_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_sailor_v1_14_0.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    sailor_v1_14_0_tests.root_module.addImport("sailor", sailor_mod);

    const run_sailor_v1_14_0_tests = b.addRunArtifact(sailor_v1_14_0_tests);
    test_step.dependOn(&run_sailor_v1_14_0_tests.step);

    // Sailor v1.15.0 feature tests (Iteration 106)
    const sailor_v1_15_0_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_sailor_v1_15_0.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    sailor_v1_15_0_tests.root_module.addImport("sailor", sailor_mod);

    const run_sailor_v1_15_0_tests = b.addRunArtifact(sailor_v1_15_0_tests);
    test_step.dependOn(&run_sailor_v1_15_0_tests.step);

    // Sailor v1.16.0 feature tests (Iteration 110)
    const sailor_v1_16_0_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_sailor_v1_16_0.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    sailor_v1_16_0_tests.root_module.addImport("sailor", sailor_mod);

    const run_sailor_v1_16_0_tests = b.addRunArtifact(sailor_v1_16_0_tests);
    test_step.dependOn(&run_sailor_v1_16_0_tests.step);

    // Lua scripting integration tests (Iteration 105)
    const lua_scripting_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_lua_scripting.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_lua_scripting_tests = b.addRunArtifact(lua_scripting_tests);
    integration_test_step.dependOn(&run_lua_scripting_tests.step);

    // Lua timeout integration tests (Iteration 111)
    const lua_timeout_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_lua_timeout.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_lua_timeout_tests = b.addRunArtifact(lua_timeout_tests);
    integration_test_step.dependOn(&run_lua_timeout_tests.step);

    // SCRIPT KILL integration tests (Iteration 112)
    const script_kill_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_script_kill.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_script_kill_tests = b.addRunArtifact(script_kill_tests);
    integration_test_step.dependOn(&run_script_kill_tests.step);

    // Lua libraries integration tests (Iteration 113)
    const lua_libraries_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_lua_libraries.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_lua_libraries_tests = b.addRunArtifact(lua_libraries_tests);
    integration_test_step.dependOn(&run_lua_libraries_tests.step);

    // ACL dispatcher integration tests (Iteration 117)
    const acl_dispatcher_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_acl_dispatcher.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_acl_dispatcher_tests = b.addRunArtifact(acl_dispatcher_tests);
    integration_test_step.dependOn(&run_acl_dispatcher_tests.step);

    // Multi-database integration tests (Iteration 126)
    const multi_database_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_multi_database.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_multi_database_tests = b.addRunArtifact(multi_database_tests);
    integration_test_step.dependOn(&run_multi_database_tests.step);

    // CLUSTER MYSHARDID integration tests (Iteration 149)
    const cluster_myshardid_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_cluster_myshardid.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_cluster_myshardid_tests = b.addRunArtifact(cluster_myshardid_tests);
    integration_test_step.dependOn(&run_cluster_myshardid_tests.step);

    const cluster_count_failure_reports_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_cluster_count_failure_reports.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_cluster_count_failure_reports_tests = b.addRunArtifact(cluster_count_failure_reports_tests);
    integration_test_step.dependOn(&run_cluster_count_failure_reports_tests.step);

    const cluster_reset_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_cluster_reset.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_cluster_reset_tests = b.addRunArtifact(cluster_reset_tests);
    integration_test_step.dependOn(&run_cluster_reset_tests.step);

    const cluster_set_config_epoch_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_cluster_set_config_epoch.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_cluster_set_config_epoch_tests = b.addRunArtifact(cluster_set_config_epoch_tests);
    integration_test_step.dependOn(&run_cluster_set_config_epoch_tests.step);

    const cluster_slot_stats_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_cluster_slot_stats.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_cluster_slot_stats_tests = b.addRunArtifact(cluster_slot_stats_tests);
    integration_test_step.dependOn(&run_cluster_slot_stats_tests.step);

    // Sentinel integration tests
    const sentinel_ping_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_sentinel_ping.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_sentinel_ping_tests = b.addRunArtifact(sentinel_ping_tests);
    integration_test_step.dependOn(&run_sentinel_ping_tests.step);

    // Note: integration tests are NOT added to the main test step because they
    // spawn a server binary and require special lifecycle management.
    // Use `zig build test-integration` to run them separately.
}
