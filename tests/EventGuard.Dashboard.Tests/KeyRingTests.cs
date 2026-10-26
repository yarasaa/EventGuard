using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.Extensions.DependencyInjection;

namespace EventGuard.Dashboard.Tests;

public sealed class KeyRingTests
{
    [Fact]
    public async Task Rotation_bridge_reads_old_and_new_keys_after_replica_restart_and_rollback()
    {
        var directory = Path.Combine(Path.GetTempPath(), "eventguard-rotation-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            string WriteCertificate(string name)
            {
                using var rsa = RSA.Create(2048);
                var request = new CertificateRequest("CN=" + name, rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
                using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(2));
                var path = Path.Combine(directory, name + ".pfx");
                File.WriteAllBytes(path, certificate.Export(X509ContentType.Pfx, "rotation-test"));
                return path;
            }
            var oldCertificate = WriteCertificate("old");
            var newCertificate = WriteCertificate("new");
            Dictionary<string, string?> Configuration(string active, string? decrypt = null)
            {
                var configuration = new Dictionary<string, string?>
                {
                    ["Dashboard:KeyRingPath"] = Path.Combine(directory, "keys"),
                    ["Dashboard:KeyProtectionCertificatePath"] = active,
                    ["Dashboard:KeyProtectionCertificatePassword"] = "rotation-test"
                };
                if (decrypt is not null)
                {
                    configuration["Dashboard:KeyDecryptionCertificates:0:Path"] = decrypt;
                    configuration["Dashboard:KeyDecryptionCertificates:0:Password"] = "rotation-test";
                }
                return configuration;
            }
            static IDataProtector Protector(DashboardFactory host) => host.Services.GetRequiredService<IDataProtectionProvider>().CreateProtector("rotation-test");
            string oldToken;
            await using (var original = new DashboardFactory(overrides: Configuration(oldCertificate), persistedProtection: true))
            {
                using var client = original.Client();
                oldToken = Protector(original).Protect("before-rotation");
            }
            await using (var switched = new DashboardFactory(overrides: Configuration(newCertificate, oldCertificate), persistedProtection: true))
            {
                using var client = switched.Client();
                Assert.Equal("before-rotation", Protector(switched).Unprotect(oldToken));
                // A real new key must be created, otherwise the test only exercises the cached old key.
                switched.Services.GetRequiredService<IKeyManager>().CreateNewKey(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(90));
            }
            string newToken;
            await using (var restarted = new DashboardFactory(overrides: Configuration(newCertificate, oldCertificate), persistedProtection: true))
            {
                using var client = restarted.Client();
                newToken = Protector(restarted).Protect("after-rotation");
                Assert.Equal("before-rotation", Protector(restarted).Unprotect(oldToken));
            }
            await using (var bridge = new DashboardFactory(overrides: Configuration(oldCertificate, newCertificate), persistedProtection: true))
            {
                using var client = bridge.Client();
                Assert.Equal("before-rotation", Protector(bridge).Unprotect(oldToken));
                Assert.Equal("after-rotation", Protector(bridge).Unprotect(newToken));
            }
            await using (var oldOnly = new DashboardFactory(overrides: Configuration(oldCertificate), persistedProtection: true))
            {
                using var client = oldOnly.Client();
                Assert.Throws<CryptographicException>(() => Protector(oldOnly).Unprotect(newToken));
            }
            Assert.All(Directory.GetFiles(Path.Combine(directory, "keys"), "key-*.xml"), file =>
            {
                var xml = File.ReadAllText(file);
                Assert.Contains("EncryptedData", xml);
                Assert.DoesNotContain("<masterKey", xml);
            });
        }
        finally { Directory.Delete(directory, recursive: true); }
    }

    [Fact]
    public async Task Two_hosts_can_read_a_persisted_key_ring_encrypted_by_the_configured_certificate()
    {
        var directory = Path.Combine(Path.GetTempPath(), "eventguard-key-test-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        try
        {
            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest("CN=local-key-test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
            var file = Path.Combine(directory, "test.pfx"); await File.WriteAllBytesAsync(file, certificate.Export(X509ContentType.Pfx, "local-test"));
            var config = new Dictionary<string, string?> { ["Dashboard:KeyRingPath"] = Path.Combine(directory, "keys"),
                ["Dashboard:KeyProtectionCertificatePath"] = file, ["Dashboard:KeyProtectionCertificatePassword"] = "local-test" };
            await using var first = new DashboardFactory(overrides: config, persistedProtection: true); using var client = first.Client();
            var secret = first.Services.GetRequiredService<IDataProtectionProvider>().CreateProtector("replica-test").Protect("protected-command");
            var xml = await File.ReadAllTextAsync(Assert.Single(Directory.GetFiles(Path.Combine(directory, "keys"), "key-*.xml")));
            Assert.Contains("EncryptedData", xml); Assert.DoesNotContain("<masterKey", xml);
            await using var second = new DashboardFactory(overrides: config, persistedProtection: true); using var other = second.Client();
            Assert.Equal("protected-command", second.Services.GetRequiredService<IDataProtectionProvider>().CreateProtector("replica-test").Unprotect(secret));
        }
        finally { Directory.Delete(directory, recursive: true); }
    }
}
