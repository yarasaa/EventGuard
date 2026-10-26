using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.DataProtection.KeyManagement;

namespace EventGuard.Dashboard;

internal static class DashboardKeyRing
{
    public static void Activate(IServiceProvider services) => services.GetRequiredService<CertificateLifetime>();

    public static void Configure(IDataProtectionBuilder builder, DashboardOptions options)
    {
        if (options.KeyRingPath.Length == 0) return;
        var certificates = new List<X509Certificate2>();
        try
        {
            certificates.Add(Load(options.KeyProtectionCertificatePath, options.KeyProtectionCertificatePassword));
            foreach (var configured in options.KeyDecryptionCertificates)
                certificates.Add(Load(configured.Path, configured.Password));
            var lifetime = new CertificateLifetime(certificates);
            builder.Services.AddSingleton(_ => lifetime);
            // Resolving key-management options transfers certificate disposal to the host container.
            builder.Services.AddOptions<KeyManagementOptions>().Configure<CertificateLifetime>((_, _) => { });
            builder.PersistKeysToFileSystem(new DirectoryInfo(options.KeyRingPath))
                .ProtectKeysWithCertificate(certificates[0])
                .UnprotectKeysWithAnyCertificate(certificates.Skip(1).ToArray());
        }
        catch
        {
            foreach (var certificate in certificates) certificate.Dispose();
            throw;
        }
    }

    private static X509Certificate2 Load(string path, string password)
    {
        var flags = OperatingSystem.IsMacOS() ? X509KeyStorageFlags.DefaultKeySet : X509KeyStorageFlags.EphemeralKeySet;
#if NET8_0
        var certificate = new X509Certificate2(path, password, flags);
#else
        var certificate = X509CertificateLoader.LoadPkcs12FromFile(path, password, flags);
#endif
        if (!certificate.HasPrivateKey) { certificate.Dispose(); throw new InvalidOperationException("Key protection requires a certificate with its private key."); }
        return certificate;
    }

    private sealed class CertificateLifetime(List<X509Certificate2> certificates) : IDisposable
    {
        public void Dispose()
        {
            foreach (var certificate in certificates) certificate.Dispose();
        }
    }
}
