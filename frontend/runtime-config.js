// Optional runtime override.
// Example:
// window.CLOUDV2_API_BASE_URL = "https://api.seu-dominio.com";
(function (globalScope) {
  if (!globalScope) return;
  if (typeof globalScope.CLOUDV2_API_BASE_URL !== "string") {
    globalScope.CLOUDV2_API_BASE_URL = "https://back-cloud-monitor.duckdns.org";
  }
})(typeof window !== "undefined" ? window : undefined);
