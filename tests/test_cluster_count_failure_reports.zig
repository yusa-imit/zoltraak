const std = @import("std");
const Storage = @import("zoltraak").Storage;
const cluster_cmds = @import("zoltraak").cluster_cmds;
const cluster_mod = @import("zoltraak").cluster_mod;

// CLUSTER COUNT-FAILURE-REPORTS integration tests

test "CLUSTER COUNT-FAILURE-REPORTS - arity validation" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Too few arguments
    const args1 = [_][]const u8{ "CLUSTER", "COUNT-FAILURE-REPORTS" };
    const result1 = try cluster_cmds.cmdClusterCountFailureReports(allocator, &args1, storage, null, 0);
    defer allocator.free(result1);
    try std.testing.expect(std.mem.indexOf(u8, result1, "-ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result1, "wrong number of arguments") != null);

    // Too many arguments
    const args2 = [_][]const u8{ "CLUSTER", "COUNT-FAILURE-REPORTS", "node-id", "extra" };
    const result2 = try cluster_cmds.cmdClusterCountFailureReports(allocator, &args2, storage, null, 0);
    defer allocator.free(result2);
    try std.testing.expect(std.mem.indexOf(u8, result2, "-ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result2, "wrong number of arguments") != null);
}

test "CLUSTER COUNT-FAILURE-REPORTS - cluster disabled error" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    // Cluster is disabled by default
    try std.testing.expect(!storage.cluster.enabled);

    const node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const args = [_][]const u8{ "CLUSTER", "COUNT-FAILURE-REPORTS", node_id };
    const result = try cluster_cmds.cmdClusterCountFailureReports(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "-ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "cluster support disabled") != null);
}

test "CLUSTER COUNT-FAILURE-REPORTS - invalid node ID length" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Too short
    const args1 = [_][]const u8{ "CLUSTER", "COUNT-FAILURE-REPORTS", "short" };
    const result1 = try cluster_cmds.cmdClusterCountFailureReports(allocator, &args1, storage, null, 0);
    defer allocator.free(result1);
    try std.testing.expect(std.mem.indexOf(u8, result1, "-ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result1, "Invalid node ID") != null);

    // Too long
    const args2 = [_][]const u8{ "CLUSTER", "COUNT-FAILURE-REPORTS", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" };
    const result2 = try cluster_cmds.cmdClusterCountFailureReports(allocator, &args2, storage, null, 0);
    defer allocator.free(result2);
    try std.testing.expect(std.mem.indexOf(u8, result2, "-ERR") != null);
    try std.testing.expect(std.mem.indexOf(u8, result2, "Invalid node ID") != null);
}

test "CLUSTER COUNT-FAILURE-REPORTS - returns 0 for unknown node" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    const node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const args = [_][]const u8{ "CLUSTER", "COUNT-FAILURE-REPORTS", node_id };
    const result = try cluster_cmds.cmdClusterCountFailureReports(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return :0 (integer 0)
    try std.testing.expect(std.mem.indexOf(u8, result, ":0") != null);
}

test "CLUSTER COUNT-FAILURE-REPORTS - returns 0 for known node with no reports" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Create a node
    const node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const node = try allocator.create(cluster_mod.ClusterNode);
    node.* = try cluster_mod.ClusterNode.init(allocator, node_id, "127.0.0.1", 7000);
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    const node_key = try allocator.dupe(u8, &node_id);
    try storage.cluster.nodes.put(node_key, node);

    const args = [_][]const u8{ "CLUSTER", "COUNT-FAILURE-REPORTS", &node_id };
    const result = try cluster_cmds.cmdClusterCountFailureReports(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return :0
    try std.testing.expect(std.mem.indexOf(u8, result, ":0") != null);
}

test "CLUSTER COUNT-FAILURE-REPORTS - returns correct count with one report" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Create a node
    const node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const node = try allocator.create(cluster_mod.ClusterNode);
    node.* = try cluster_mod.ClusterNode.init(allocator, node_id, "127.0.0.1", 7000);
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    const node_key = try allocator.dupe(u8, &node_id);
    try storage.cluster.nodes.put(node_key, node);

    // Add one failure report
    const reporter = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".*;
    try storage.cluster.addFailureReport(node_id, reporter);

    const args = [_][]const u8{ "CLUSTER", "COUNT-FAILURE-REPORTS", &node_id };
    const result = try cluster_cmds.cmdClusterCountFailureReports(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return :1
    try std.testing.expect(std.mem.indexOf(u8, result, ":1") != null);
}

test "CLUSTER COUNT-FAILURE-REPORTS - returns correct count with multiple reports" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Create a node
    const node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const node = try allocator.create(cluster_mod.ClusterNode);
    node.* = try cluster_mod.ClusterNode.init(allocator, node_id, "127.0.0.1", 7000);
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    const node_key = try allocator.dupe(u8, &node_id);
    try storage.cluster.nodes.put(node_key, node);

    // Add multiple failure reports
    const reporter1 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".*;
    const reporter2 = "cccccccccccccccccccccccccccccccccccccccc".*;
    const reporter3 = "dddddddddddddddddddddddddddddddddddddddd".*;
    try storage.cluster.addFailureReport(node_id, reporter1);
    try storage.cluster.addFailureReport(node_id, reporter2);
    try storage.cluster.addFailureReport(node_id, reporter3);

    const args = [_][]const u8{ "CLUSTER", "COUNT-FAILURE-REPORTS", &node_id };
    const result = try cluster_cmds.cmdClusterCountFailureReports(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return :3
    try std.testing.expect(std.mem.indexOf(u8, result, ":3") != null);
}

test "CLUSTER COUNT-FAILURE-REPORTS - duplicate reports are counted once" {
    const allocator = std.testing.allocator;
    const storage = try Storage.init(allocator, 6379, "127.0.0.1");
    defer storage.deinit();

    storage.cluster.enabled = true;

    // Create a node
    const node_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".*;
    const node = try allocator.create(cluster_mod.ClusterNode);
    node.* = try cluster_mod.ClusterNode.init(allocator, node_id, "127.0.0.1", 7000);
    defer {
        node.deinit(allocator);
        allocator.destroy(node);
    }

    const node_key = try allocator.dupe(u8, &node_id);
    try storage.cluster.nodes.put(node_key, node);

    // Add same reporter multiple times
    const reporter = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".*;
    try storage.cluster.addFailureReport(node_id, reporter);
    try storage.cluster.addFailureReport(node_id, reporter);
    try storage.cluster.addFailureReport(node_id, reporter);

    const args = [_][]const u8{ "CLUSTER", "COUNT-FAILURE-REPORTS", &node_id };
    const result = try cluster_cmds.cmdClusterCountFailureReports(allocator, &args, storage, null, 0);
    defer allocator.free(result);

    // Should return :1 (duplicate reports ignored)
    try std.testing.expect(std.mem.indexOf(u8, result, ":1") != null);
}
