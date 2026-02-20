const std = @import("std");

pub fn build(b: *std.Build) void {
    // Target configuration
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Executable: zoltraak
    const exe = b.addExecutable(.{
        .name = "zoltraak",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    // Install step
    b.installArtifact(exe);

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

    // Note: integration tests are NOT added to the main test step because they
    // spawn a server binary and require special lifecycle management.
    // Use `zig build test-integration` to run them separately.
}
