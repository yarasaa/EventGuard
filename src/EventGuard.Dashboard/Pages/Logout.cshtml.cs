using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace EventGuard.Dashboard.Pages;

// Razor Pages verifies the antiforgery token on POST. GET never signs a user out.
public sealed class LogoutModel(DashboardOptions options) : PageModel
{
    public async Task<IActionResult> OnPostAsync()
    {
        if (options.DevelopmentAccess) return RedirectToPage("/Index");
        await HttpContext.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
        return RedirectToPage("/SignedOut");
    }
}
