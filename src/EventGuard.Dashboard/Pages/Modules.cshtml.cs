using EventGuard.Operations;
using Microsoft.AspNetCore.Mvc.RazorPages;
namespace EventGuard.Dashboard.Pages;
public sealed class ModulesModel(DashboardModules modules) : PageModel
{
    public ModuleView View { get; private set; } = null!;
    public IReadOnlyList<ModuleRegistration> Targets => modules.Allowed(User);
    public Task OnGetAsync(string? target, int? state, string? id, string? cursor) => Load(target, state, id, cursor);
    private async Task Load(string? target, int? state, string? id, string? cursor) => View = await modules.ReadAsync(User, target, state, id, cursor, HttpContext.RequestAborted);
    public string Location(string cursor) => "/modules" + QueryString.Create(new Dictionary<string,string?> { ["target"] = View.Registration.Key,
        ["state"] = View.State?.ToString(System.Globalization.CultureInfo.InvariantCulture), ["id"] = View.Id?.ToString("D"), ["cursor"] = cursor });
}
