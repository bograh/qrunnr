package views

// Shared Tailwind class lists for the dashboard's design system (DESIGN.md).
// Kept as literals so the Tailwind scanner sees every class. Colours come
// only from the locked tokens in web/admin/tailwind.css.
const (
	// Surfaces: flat panels separated by rules; no shadows, square corners.
	panelClass     = "border border-rule bg-paper"
	panelHeadClass = "flex flex-wrap items-center justify-between gap-x-4 gap-y-2 border-b border-rule px-4 py-2.5"
	panelTitle     = "text-[13px] font-semibold text-ink"
	panelMeta      = "font-mono text-[11px] text-ink-3"

	pageTitleClass = "text-[22px] leading-7 font-semibold tracking-[-0.015em] text-ink"
	pageLedeClass  = "mt-1 max-w-[70ch] text-[13px] leading-5 text-ink-2"

	// Buttons: default · hover · focus-visible (global ring) · active · disabled · loading (htmx-request).
	btnBase      = "inline-flex h-8 items-center justify-center gap-1.5 rounded-[2px] px-3 text-[13px] font-medium whitespace-nowrap transition-colors duration-150 ease-out active:translate-y-px disabled:cursor-not-allowed disabled:opacity-40 [&.htmx-request]:pointer-events-none [&.htmx-request]:opacity-60"
	btnPrimary   = btnBase + " bg-ink text-paper hover:bg-ink-2"
	btnSecondary = btnBase + " border border-rule-2 bg-paper text-ink hover:bg-paper-2"
	btnDanger    = btnBase + " border border-fail/40 bg-paper text-fail hover:bg-fail/8"
	btnGhost     = "inline-flex h-7 items-center rounded-[2px] px-2 text-[12px] font-medium text-ink-2 transition-colors duration-150 ease-out hover:bg-paper-3 hover:text-ink active:translate-y-px [&.htmx-request]:opacity-60"
	btnGhostFail = "inline-flex h-7 items-center rounded-[2px] px-2 text-[12px] font-medium text-fail transition-colors duration-150 ease-out hover:bg-fail/8 active:translate-y-px [&.htmx-request]:opacity-60"

	// Console-style fields: a mono prefix cell labels the control.
	fieldClass       = "flex h-8 min-w-0 items-stretch border border-rule-2 bg-paper focus-within:border-ink-3"
	fieldPrefixClass = "flex shrink-0 items-center border-r border-rule bg-paper-2 px-2 font-mono text-[11px] text-ink-3"
	fieldInputClass  = "min-w-0 flex-1 bg-transparent px-2 text-[13px] text-ink outline-none placeholder:text-ink-3/70"
	inputClass       = "h-8 w-full border border-rule-2 bg-paper px-2 text-[13px] text-ink outline-none placeholder:text-ink-3/70 focus:border-ink-3"
	inputNumClass    = "h-8 w-24 border border-rule-2 bg-paper px-2 font-mono text-[13px] tabular-nums text-ink outline-none placeholder:font-sans placeholder:text-ink-3/70 focus:border-ink-3"

	// Tables: 36px rows, tabular numerals, numbers right-aligned.
	tableWrapClass = "overflow-x-auto"
	tableClass     = "w-full border-collapse text-[13px]"
	thClass        = "h-8 border-b border-rule bg-paper-2 px-3 text-left text-[12px] font-medium whitespace-nowrap text-ink-3"
	thNumClass     = "h-8 border-b border-rule bg-paper-2 px-3 text-right text-[12px] font-medium whitespace-nowrap text-ink-3"
	trClass        = "border-b border-rule text-ink last:border-b-0 hover:bg-paper-2"
	tdClass        = "h-9 px-3 whitespace-nowrap"
	tdNumClass     = "h-9 px-3 text-right font-mono text-[12.5px] whitespace-nowrap tabular-nums"
	tdMutedClass   = "h-9 px-3 whitespace-nowrap text-ink-3"

	idLinkClass   = "font-mono text-[12.5px] text-ink underline decoration-rule-2 underline-offset-[3px] hover:decoration-ink"
	textLinkClass = "text-ink underline decoration-rule-2 underline-offset-[3px] hover:decoration-ink"
	mutedDash     = "text-ink-3"
	emptyClass    = "px-4 py-10 text-[13px] text-ink-2"
	checkboxClass = "size-3.5 accent-ink"
)
