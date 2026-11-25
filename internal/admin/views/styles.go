package views

// Shared Tailwind class lists. Kept as literals so the Tailwind scanner sees
// every class.
const (
	cardClass  = "rounded-lg border border-zinc-200 bg-white shadow-xs dark:border-zinc-800 dark:bg-zinc-900"
	inputClass = "block w-full rounded-md border-0 bg-white px-2.5 py-1.5 text-sm text-zinc-900 shadow-xs ring-1 ring-inset ring-zinc-300 placeholder:text-zinc-400 dark:placeholder:text-zinc-600 focus:ring-2 focus:ring-inset focus:ring-zinc-900 dark:bg-zinc-950 dark:text-zinc-100 dark:ring-zinc-700 dark:focus:ring-zinc-300"
	// compactInputClass is inputClass without w-full, for fixed-width inline inputs.
	compactInputClass = "block w-28 shrink-0 rounded-md border-0 bg-white px-2.5 py-1.5 text-sm text-zinc-900 shadow-xs ring-1 ring-inset ring-zinc-300 placeholder:text-zinc-400 dark:placeholder:text-zinc-600 focus:ring-2 focus:ring-inset focus:ring-zinc-900 dark:bg-zinc-950 dark:text-zinc-100 dark:ring-zinc-700 dark:focus:ring-zinc-300"
	labelClass        = "mb-1 block text-xs font-medium text-zinc-600 dark:text-zinc-400"
	btnPrimary        = "inline-flex items-center justify-center gap-1.5 rounded-md bg-zinc-900 px-3 py-1.5 text-sm font-medium text-white shadow-xs hover:bg-zinc-700 disabled:opacity-50 dark:bg-white dark:text-zinc-900 dark:hover:bg-zinc-200"
	btnSecondary      = "inline-flex items-center justify-center gap-1.5 rounded-md bg-white px-3 py-1.5 text-sm font-medium text-zinc-700 shadow-xs ring-1 ring-inset ring-zinc-300 hover:bg-zinc-50 disabled:opacity-50 dark:bg-zinc-900 dark:text-zinc-200 dark:ring-zinc-700 dark:hover:bg-zinc-800"
	btnDanger         = "inline-flex items-center justify-center gap-1.5 rounded-md bg-white px-3 py-1.5 text-sm font-medium text-red-700 shadow-xs ring-1 ring-inset ring-red-300 hover:bg-red-50 disabled:opacity-50 dark:bg-zinc-900 dark:text-red-300 dark:ring-red-400/40 dark:hover:bg-red-400/10"
	btnSmall          = "inline-flex items-center rounded px-2 py-1 text-xs font-medium text-zinc-700 ring-1 ring-inset ring-zinc-300 hover:bg-zinc-50 dark:text-zinc-200 dark:ring-zinc-700 dark:hover:bg-zinc-800"
	btnSmallDanger    = "inline-flex items-center rounded px-2 py-1 text-xs font-medium text-red-700 ring-1 ring-inset ring-red-300 hover:bg-red-50 dark:text-red-300 dark:ring-red-400/40 dark:hover:bg-red-400/10"
	thClass           = "px-3 py-2 text-left text-xs font-medium uppercase tracking-wide text-zinc-500 dark:text-zinc-400"
	thRightClass      = "px-3 py-2 text-right text-xs font-medium uppercase tracking-wide text-zinc-500 dark:text-zinc-400"
	tdClass           = "px-3 py-2 text-sm text-zinc-700 dark:text-zinc-300 whitespace-nowrap"
	tdRightClass      = "px-3 py-2 text-right text-sm tabular-nums text-zinc-700 dark:text-zinc-300 whitespace-nowrap"
	linkClass         = "font-medium text-zinc-900 underline decoration-zinc-300 underline-offset-2 hover:decoration-zinc-900 dark:text-zinc-100 dark:decoration-zinc-600 dark:hover:decoration-zinc-200"
	monoLinkClass     = "font-mono text-xs text-zinc-900 underline decoration-zinc-300 underline-offset-2 hover:decoration-zinc-900 dark:text-zinc-100 dark:decoration-zinc-600 dark:hover:decoration-zinc-200"
	tableWrapClass    = "overflow-x-auto"
	tableClass        = "min-w-full divide-y divide-zinc-200 dark:divide-zinc-800"
	tbodyClass        = "divide-y divide-zinc-100 dark:divide-zinc-800/70"
	mutedClass        = "text-zinc-400 dark:text-zinc-500"
	sectionTitle      = "text-sm font-semibold text-zinc-900 dark:text-zinc-100"
	pageTitleClass    = "text-xl font-semibold tracking-tight text-zinc-900 dark:text-zinc-50"
	pageSubClass      = "mt-1 text-sm text-zinc-500 dark:text-zinc-400"
	emptyStateClass   = "px-3 py-10 text-center text-sm text-zinc-500 dark:text-zinc-400"
)
