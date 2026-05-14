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

    // Create main module for sharing across tests
    const zoltraak_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    zoltraak_mod.addImport("sailor", sailor_mod);
    zoltraak_mod.addImport("zuda", zuda_mod);

    // Executable: zoltraak (server)
    const exe = b.addExecutable(.{
        .name = "zoltraak",
        .root_module = zoltraak_mod,
    });

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

    // Hash increment tests (HINCRBY/HINCRBYFLOAT)
    const hash_incr_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_hash_incr.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_hash_incr_tests = b.addRunArtifact(hash_incr_tests);
    integration_test_step.dependOn(&run_hash_incr_tests.step);

    // BF.INFO tests
    const bf_info_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_bf_info.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_bf_info_tests = b.addRunArtifact(bf_info_tests);
    integration_test_step.dependOn(&run_bf_info_tests.step);

    // BF.CARD tests
    const bf_card_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_bf_card.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_bf_card_tests = b.addRunArtifact(bf_card_tests);
    integration_test_step.dependOn(&run_bf_card_tests.step);

    // Bloom Filter SCANDUMP/LOADCHUNK integration tests (Iteration 215)
    const bf_scandump_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_bloom_scandump.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_bf_scandump_tests = b.addRunArtifact(bf_scandump_tests);
    integration_test_step.dependOn(&run_bf_scandump_tests.step);

    // Cuckoo Filter CF.COUNT integration tests (Iteration 219)
    const cf_count_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_cf_count.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_cf_count_tests = b.addRunArtifact(cf_count_tests);
    integration_test_step.dependOn(&run_cf_count_tests.step);

    // CF.INFO/SCANDUMP/LOADCHUNK integration tests (Iteration 220)
    const cf_info_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_cf_info_scandump_loadchunk.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_cf_info_tests = b.addRunArtifact(cf_info_tests);
    integration_test_step.dependOn(&run_cf_info_tests.step);

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

    // XINFO STREAM IDMP fields integration tests (Iteration 244)
    const xinfo_stream_idmp_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_xinfo_stream_idmp.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_xinfo_stream_idmp_tests = b.addRunArtifact(xinfo_stream_idmp_tests);
    integration_test_step.dependOn(&run_xinfo_stream_idmp_tests.step);

    // Blocking sorted set notification tests (Iteration 250)
    const blocking_zset_notification_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_blocking_zset_notifications.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_blocking_zset_notification_tests = b.addRunArtifact(blocking_zset_notification_tests);
    integration_test_step.dependOn(&run_blocking_zset_notification_tests.step);

    // Set notification tests (Iteration 251)
    const set_notification_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_set_notifications.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_set_notification_tests = b.addRunArtifact(set_notification_tests);
    integration_test_step.dependOn(&run_set_notification_tests.step);

    // Stream notification tests (Iteration 252)
    const stream_notification_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_stream_notifications.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_stream_notification_tests = b.addRunArtifact(stream_notification_tests);
    integration_test_step.dependOn(&run_stream_notification_tests.step);

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

    // Client tracking table unit tests (Iteration 243)
    const client_tracking_table_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_client_tracking_table.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zoltraak", .module = zoltraak_mod },
            },
        }),
    });
    client_tracking_table_tests.linkSystemLibrary("luajit-5.1");
    client_tracking_table_tests.linkLibC();
    client_tracking_table_tests.addIncludePath(.{ .cwd_relative = "/opt/homebrew/opt/luajit/include/luajit-2.1" });
    client_tracking_table_tests.addLibraryPath(.{ .cwd_relative = "/opt/homebrew/opt/luajit/lib" });

    const run_client_tracking_table_tests = b.addRunArtifact(client_tracking_table_tests);
    test_step.dependOn(&run_client_tracking_table_tests.step);

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

    // Sailor v2.5.0 feature tests (Iteration 251)
    const sailor_v2_5_0_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_sailor_v2_5_0.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    sailor_v2_5_0_tests.root_module.addImport("sailor", sailor_mod);

    const run_sailor_v2_5_0_tests = b.addRunArtifact(sailor_v2_5_0_tests);
    test_step.dependOn(&run_sailor_v2_5_0_tests.step);

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

    // Redis Functions integration tests
    const functions_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_functions.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_functions_tests = b.addRunArtifact(functions_tests);
    integration_test_step.dependOn(&run_functions_tests.step);

    // JSON integration tests
    const json_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_json.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_json_tests = b.addRunArtifact(json_tests);
    integration_test_step.dependOn(&run_json_tests.step);

    // JSON.CLEAR integration tests (Iteration 174)
    const json_clear_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_json_clear.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_json_clear_tests = b.addRunArtifact(json_clear_tests);
    integration_test_step.dependOn(&run_json_clear_tests.step);

    // JSON.ARRLEN integration tests (Iteration 177)
    const json_arrlen_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_json_arrlen.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_json_arrlen_tests = b.addRunArtifact(json_arrlen_tests);
    integration_test_step.dependOn(&run_json_arrlen_tests.step);

    // JSON.ARRPOP integration tests (Iteration 178)
    const json_arrpop_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_json_arrpop.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_json_arrpop_tests = b.addRunArtifact(json_arrpop_tests);
    integration_test_step.dependOn(&run_json_arrpop_tests.step);

    // JSON.ARRTRIM integration tests (Iteration 179)
    const json_arrtrim_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_json_arrtrim.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_json_arrtrim_tests = b.addRunArtifact(json_arrtrim_tests);
    integration_test_step.dependOn(&run_json_arrtrim_tests.step);

    // JSON.RESP integration tests (Iteration 182)
    const json_resp_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_json_resp.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_json_resp_tests = b.addRunArtifact(json_resp_tests);
    integration_test_step.dependOn(&run_json_resp_tests.step);

    // JSON.DEBUG integration tests (Iteration 184)
    const json_debug_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_json_debug.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_json_debug_tests = b.addRunArtifact(json_debug_tests);
    integration_test_step.dependOn(&run_json_debug_tests.step);

    // Search (FT.*) integration tests
    const search_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_search.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_search_tests = b.addRunArtifact(search_tests);
    integration_test_step.dependOn(&run_search_tests.step);

    // FT.ALTER integration tests
    const ft_alter_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ft_alter.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_ft_alter_tests = b.addRunArtifact(ft_alter_tests);
    integration_test_step.dependOn(&run_ft_alter_tests.step);

    // FT.AGGREGATE integration tests
    const ft_aggregate_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ft_aggregate.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_ft_aggregate_tests = b.addRunArtifact(ft_aggregate_tests);
    integration_test_step.dependOn(&run_ft_aggregate_tests.step);

    // FT.EXPLAIN integration tests
    const ft_explain_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ft_explain.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_ft_explain_tests = b.addRunArtifact(ft_explain_tests);
    integration_test_step.dependOn(&run_ft_explain_tests.step);

    // FT.EXPLAINCLI integration tests
    const ft_explaincli_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ft_explaincli.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_ft_explaincli_tests = b.addRunArtifact(ft_explaincli_tests);
    integration_test_step.dependOn(&run_ft_explaincli_tests.step);

    // FT.PROFILE integration tests
    const ft_profile_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ft_profile.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_ft_profile_tests = b.addRunArtifact(ft_profile_tests);
    integration_test_step.dependOn(&run_ft_profile_tests.step);

    // FT.SPELLCHECK integration tests
    const ft_spellcheck_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ft_spellcheck.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_ft_spellcheck_tests = b.addRunArtifact(ft_spellcheck_tests);
    integration_test_step.dependOn(&run_ft_spellcheck_tests.step);

    // FT.CURSOR integration tests
    const ft_cursor_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ft_cursor.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_ft_cursor_tests = b.addRunArtifact(ft_cursor_tests);
    integration_test_step.dependOn(&run_ft_cursor_tests.step);

    // FT.ALIAS* commands integration tests (Iteration 195)
    const ft_alias_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ft_alias.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_ft_alias_tests = b.addRunArtifact(ft_alias_tests);
    integration_test_step.dependOn(&run_ft_alias_tests.step);

    // FT.DICT* commands integration tests (Iteration 196)
    const ft_dict_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ft_dict.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_ft_dict_tests = b.addRunArtifact(ft_dict_tests);
    integration_test_step.dependOn(&run_ft_dict_tests.step);

    // FT.SYNDUMP/FT.SYNUPDATE integration tests (Iteration 197)
    const ft_synonym_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ft_synonym.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_ft_synonym_tests = b.addRunArtifact(ft_synonym_tests);
    integration_test_step.dependOn(&run_ft_synonym_tests.step);

    // FT.SUG* (auto-complete suggestions) integration tests
    const ft_suggestions_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ft_suggestions.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_ft_suggestions_tests = b.addRunArtifact(ft_suggestions_tests);
    integration_test_step.dependOn(&run_ft_suggestions_tests.step);

    // FT.TAGVALS integration tests (Iteration 199)
    const ft_tagvals_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ft_tagvals.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_ft_tagvals_tests = b.addRunArtifact(ft_tagvals_tests);
    integration_test_step.dependOn(&run_ft_tagvals_tests.step);

    // FT.CONFIG integration tests (Iteration 200)
    const ft_config_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ft_config.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_ft_config_tests = b.addRunArtifact(ft_config_tests);
    integration_test_step.dependOn(&run_ft_config_tests.step);

    // FT.HYBRID integration tests (Iteration 201)
    const ft_hybrid_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ft_hybrid.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_ft_hybrid_tests = b.addRunArtifact(ft_hybrid_tests);
    integration_test_step.dependOn(&run_ft_hybrid_tests.step);

    // HOTKEYS integration tests (Iteration 246)
    const hotkeys_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_hotkeys.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_hotkeys_tests = b.addRunArtifact(hotkeys_tests);
    integration_test_step.dependOn(&run_hotkeys_tests.step);

    // Time Series integration tests
    const timeseries_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_timeseries.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_timeseries_tests = b.addRunArtifact(timeseries_tests);
    integration_test_step.dependOn(&run_timeseries_tests.step);

    // Bloom Filter batch commands integration tests (Iteration 211)
    const bf_batch_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_bf_batch.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_bf_batch_tests = b.addRunArtifact(bf_batch_tests);
    integration_test_step.dependOn(&run_bf_batch_tests.step);

    // Bloom Filter BF.INSERT integration tests (Iteration 212)
    const bf_insert_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_bf_insert.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_bf_insert_tests = b.addRunArtifact(bf_insert_tests);
    integration_test_step.dependOn(&run_bf_insert_tests.step);

    // Cuckoo Filter batch commands integration tests (Iteration 216)
    const cuckoo_batch_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_cuckoo_batch.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_cuckoo_batch_tests = b.addRunArtifact(cuckoo_batch_tests);
    integration_test_step.dependOn(&run_cuckoo_batch_tests.step);

    // Function DUMP/RESTORE integration tests
    const function_dump_restore_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_function_dump_restore.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_function_dump_restore_tests = b.addRunArtifact(function_dump_restore_tests);
    integration_test_step.dependOn(&run_function_dump_restore_tests.step);

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

    const sentinel_masters_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_sentinel_masters.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_sentinel_masters_tests = b.addRunArtifact(sentinel_masters_tests);
    integration_test_step.dependOn(&run_sentinel_masters_tests.step);

    const sentinel_master_addr_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_sentinel_master_addr.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_sentinel_master_addr_tests = b.addRunArtifact(sentinel_master_addr_tests);
    integration_test_step.dependOn(&run_sentinel_master_addr_tests.step);

    const sentinel_sentinels_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_sentinel_sentinels.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_sentinel_sentinels_tests = b.addRunArtifact(sentinel_sentinels_tests);
    integration_test_step.dependOn(&run_sentinel_sentinels_tests.step);

    const sentinel_reset_failover_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_sentinel.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_sentinel_reset_failover_tests = b.addRunArtifact(sentinel_reset_failover_tests);
    integration_test_step.dependOn(&run_sentinel_reset_failover_tests.step);

    const sentinel_iteration_160_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_sentinel_iteration_160.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_sentinel_iteration_160_tests = b.addRunArtifact(sentinel_iteration_160_tests);
    integration_test_step.dependOn(&run_sentinel_iteration_160_tests.step);

    const sentinel_config_myid_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_sentinel_config_myid.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_sentinel_config_myid_tests = b.addRunArtifact(sentinel_config_myid_tests);
    integration_test_step.dependOn(&run_sentinel_config_myid_tests.step);

    const sentinel_final_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_sentinel_final.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_sentinel_final_tests = b.addRunArtifact(sentinel_final_tests);
    integration_test_step.dependOn(&run_sentinel_final_tests.step);

    // Module integration tests (Phase 17)
    const modules_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_modules.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_modules_tests = b.addRunArtifact(modules_tests);
    integration_test_step.dependOn(&run_modules_tests.step);

    const module_commands_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_module_commands.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_module_commands_tests = b.addRunArtifact(module_commands_tests);
    integration_test_step.dependOn(&run_module_commands_tests.step);

    const module_commands_execution_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_module_commands_execution.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_module_commands_execution_tests = b.addRunArtifact(module_commands_execution_tests);
    integration_test_step.dependOn(&run_module_commands_execution_tests.step);

    const module_datatypes_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_module_datatypes.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_module_datatypes_tests = b.addRunArtifact(module_datatypes_tests);
    integration_test_step.dependOn(&run_module_datatypes_tests.step);

    // Module hooks unit tests
    const module_hooks_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_module_hooks.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_module_hooks_tests = b.addRunArtifact(module_hooks_tests);
    integration_test_step.dependOn(&run_module_hooks_tests.step);

    // Module hooks integration tests
    const module_hooks_integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_module_hooks_integration.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_module_hooks_integration_tests = b.addRunArtifact(module_hooks_integration_tests);
    integration_test_step.dependOn(&run_module_hooks_integration_tests.step);

    // Module timers tests (Iteration 261)
    const module_timers_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_module_timers.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_module_timers_tests = b.addRunArtifact(module_timers_tests);
    integration_test_step.dependOn(&run_module_timers_tests.step);

    // Time Series TS.ADD/TS.MADD integration tests
    const ts_add_madd_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ts_add_madd.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_ts_add_madd_tests = b.addRunArtifact(ts_add_madd_tests);
    integration_test_step.dependOn(&run_ts_add_madd_tests.step);

    // Time Series TS.INCRBY/TS.DECRBY integration tests
    const ts_incrby_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ts_incrby.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_ts_incrby_tests = b.addRunArtifact(ts_incrby_tests);
    integration_test_step.dependOn(&run_ts_incrby_tests.step);

    // Time Series TS.DEL/TS.GET integration tests
    const ts_del_get_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ts_del_get.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_ts_del_get_tests = b.addRunArtifact(ts_del_get_tests);
    integration_test_step.dependOn(&run_ts_del_get_tests.step);

    // Time Series TS.RANGE/TS.REVRANGE integration tests
    const ts_range_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ts_range.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_ts_range_tests = b.addRunArtifact(ts_range_tests);
    integration_test_step.dependOn(&run_ts_range_tests.step);

    // Time Series TS.MRANGE/TS.MREVRANGE integration tests
    const ts_mrange_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_ts_mrange.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_ts_mrange_tests = b.addRunArtifact(ts_mrange_tests);
    integration_test_step.dependOn(&run_ts_mrange_tests.step);

    // CMS integration tests
    const cms_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_cms.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_cms_tests = b.addRunArtifact(cms_tests);
    integration_test_step.dependOn(&run_cms_tests.step);

    // Top-K integration tests
    const topk_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_topk.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_topk_tests = b.addRunArtifact(topk_tests);
    integration_test_step.dependOn(&run_topk_tests.step);

    // Top-K extended integration tests (INCRBY/LIST/INFO)
    const topk_extended_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_topk_extended.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_topk_extended_tests = b.addRunArtifact(topk_extended_tests);
    integration_test_step.dependOn(&run_topk_extended_tests.step);

    const tdigest_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_tdigest.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_tdigest_tests = b.addRunArtifact(tdigest_tests);
    integration_test_step.dependOn(&run_tdigest_tests.step);

    const vector_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_vectors.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_vector_tests = b.addRunArtifact(vector_tests);
    integration_test_step.dependOn(&run_vector_tests.step);

    const vector_ops_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_vector_operations.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_vector_ops_tests = b.addRunArtifact(vector_ops_tests);
    integration_test_step.dependOn(&run_vector_ops_tests.step);

    const keyspace_notif_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_keyspace_notifications.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_keyspace_notif_tests = b.addRunArtifact(keyspace_notif_tests);
    integration_test_step.dependOn(&run_keyspace_notif_tests.step);

    const eviction_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_eviction.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_eviction_tests = b.addRunArtifact(eviction_tests);
    integration_test_step.dependOn(&run_eviction_tests.step);

    const lazyfree_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_lazyfree.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_lazyfree_tests = b.addRunArtifact(lazyfree_tests);
    integration_test_step.dependOn(&run_lazyfree_tests.step);

    const deprecated_aliases_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_deprecated_aliases.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_deprecated_aliases_tests = b.addRunArtifact(deprecated_aliases_tests);
    integration_test_step.dependOn(&run_deprecated_aliases_tests.step);

    const defrag_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_defrag.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_defrag_tests = b.addRunArtifact(defrag_tests);
    integration_test_step.dependOn(&run_defrag_tests.step);

    const encodings_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_encodings.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_encodings_tests = b.addRunArtifact(encodings_tests);
    integration_test_step.dependOn(&run_encodings_tests.step);

    // TLS CONFIG GET/SET integration tests (Iteration 252)
    const config_tls_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_config_tls.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_config_tls_tests = b.addRunArtifact(config_tls_tests);
    integration_test_step.dependOn(&run_config_tls_tests.step);

    // TLS socket initialization and handshake tests (Iteration 253)
    const tls_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_tls.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_tls_tests = b.addRunArtifact(tls_tests);
    integration_test_step.dependOn(&run_tls_tests.step);

    // DELEX command integration tests
    const delex_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/test_delex.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_delex_tests = b.addRunArtifact(delex_tests);
    integration_test_step.dependOn(&run_delex_tests.step);

    // Note: integration tests are NOT added to the main test step because they
    // spawn a server binary and require special lifecycle management.
    // Use `zig build test-integration` to run them separately.
}
