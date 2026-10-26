#!/usr/bin/env python3
"""Read-only OpenShift checks against an explicit context and namespace. Never reads Secret contents."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--output", required=True, type=Path, help="New JSON evidence file.")
    args = parser.parse_args()
    cli = shutil.which("oc") or shutil.which("kubectl")
    if not cli:
        parser.error("Install the official OpenShift CLI (oc) or kubectl first.")
    if args.output.exists():
        parser.error("Choose a new output file; existing evidence is never overwritten.")
    base = [cli, "--context", args.context, "--namespace", args.namespace, "--request-timeout=15s"]
    evidence = {"context": args.context, "namespace": args.namespace, "readOnly": True,
                "clusterTestsPerformed": False, "checks": {}}

    def query(name, command, structured=False):
        try:
            result = subprocess.run(base + command, text=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, timeout=25)
            # Do not copy raw kubeconfig, error messages, credentials or exec-plugin output into evidence.
            check = {"ok": result.returncode == 0, "exitCode": result.returncode}
            if result.returncode == 0:
                check["value"] = json.loads(result.stdout) if structured else result.stdout.strip()
        except (subprocess.TimeoutExpired, json.JSONDecodeError):
            check = {"ok": False, "error": "timeout-or-invalid-response"}
        evidence["checks"][name] = check
        return check

    try:
        routes = query("routesApi", ["api-resources", "--api-group=route.openshift.io", "-o", "name"])
        evidence["openShiftDetected"] = routes.get("ok", False) and "routes.route.openshift.io" in routes.get("value", "").splitlines()
        permissions = {}
        for resource in ("deployments.apps", "jobs.batch", "services", "configmaps", "secrets", "persistentvolumeclaims", "routes.route.openshift.io"):
            result = query("create:" + resource, ["auth", "can-i", "create", resource])
            permissions[resource] = result.get("ok", False) and result.get("value") == "yes"
        # JSONPath restricts output to quota/resource limits and avoids arbitrary annotations/labels.
        query("quota", ["get", "resourcequotas", "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\t\"}{.status.hard}{\"\\t\"}{.status.used}{\"\\n\"}{end}"])
        query("limits", ["get", "limitranges", "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\t\"}{.spec.limits}{\"\\n\"}{end}"])
        node_access = query("listNodesPermission", ["auth", "can-i", "list", "nodes"])
        evidence["canObserveClusterNodes"] = node_access.get("ok", False) and node_access.get("value") == "yes"
        evidence["deploymentPermissionsAvailable"] = evidence["openShiftDetected"] and all(permissions.values())
        evidence["remainingLiveChecks"] = [
            "Select storage that supports the required key-ring sharing and backup policy",
            "Build/push images, migrate the test database, deploy under restricted SCC and verify arbitrary UID",
            "Configure a real OIDC client and test login, permissions, logout and replica restart",
            "Verify service-CA/router TLS renewal and Data Protection certificate rotation separately",
            "Restore a native backup and reconcile deduplication, replay receipts, outbox and business effects",
            "Kill a worker during processing and verify recovery; separately test node loss with cluster-admin authorization",
            "Run representative sustained/burst load; Sandbox quota measurements are not production capacity"
        ]
    finally:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x") as target:
            json.dump(evidence, target, indent=2)
            target.write("\n")
    print("Preflight saved. No deployments or cluster failure tests were performed.")
    return 0 if evidence.get("deploymentPermissionsAvailable") else 1


if __name__ == "__main__":
    raise SystemExit(main())
