// Dead-letter selection: keeps "N selected" and the bulk buttons in sync with
// the checkboxes, including after htmx swaps the results.
(function () {
  function sync(scope) {
    if (!scope) return;
    var boxes = scope.querySelectorAll('input[name="selected"]');
    var checked = scope.querySelectorAll('input[name="selected"]:checked').length;
    var counter = scope.querySelector("[data-selection-count]");
    if (counter) counter.textContent = checked === 0 ? "none selected" : checked + " selected";
    scope.querySelectorAll("[data-requires-selection]").forEach(function (b) {
      b.disabled = checked === 0;
    });
    var all = scope.querySelector("[data-select-all]");
    if (all) {
      all.checked = boxes.length > 0 && checked === boxes.length;
      all.indeterminate = checked > 0 && checked < boxes.length;
    }
  }

  document.addEventListener("change", function (e) {
    var scope = e.target.closest("[data-selection-scope]");
    if (!scope) return;
    if (e.target.matches("[data-select-all]")) {
      scope.querySelectorAll('input[name="selected"]').forEach(function (c) {
        c.checked = e.target.checked;
      });
    }
    sync(scope);
  });

  function syncAll() {
    document.querySelectorAll("[data-selection-scope]").forEach(sync);
  }
  document.addEventListener("DOMContentLoaded", syncAll);
  document.addEventListener("htmx:afterSettle", syncAll);
})();
