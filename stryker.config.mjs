/**
 * Stryker mutation testing configuration.
 *
 * Scope: the three highest-risk modules —
 *   - src/protocol.ts  (wire frame parsing/encoding)
 *   - src/registry.ts  (peer registry + auth)
 *   - src/guard.ts     (abuse/flood protection)
 *
 * Concurrency is roughly half the logical core count (12 cores -> 6) so that
 * real named-pipe/socket e2e tests keep enough CPU headroom under load.
 * `perTest` coverage analysis makes each mutant run only the tests that cover
 * it, instead of the full 581-test suite every time.
 */
import os from "node:os";

const cores = os.cpus().length;
const concurrency = Math.max(2, Math.floor(cores / 2)); // 12 -> 6

export default {
	$concurrency: concurrency,
	packageManager: "npm",
	testRunner: "vitest",
	vitest: {
		configFile: "./vitest.config.ts",
	},
	coverageAnalysis: "perTest",
	mutate: [
		"src/protocol.ts",
		"src/registry.ts",
		"src/guard.ts",
	],
	timeoutMS: 60000,
	timeoutFactor: 3,
	reporters: ["html", "clear-text", "json"],
	htmlReporter: {
		fileName: "reports/mutation/index.html",
	},
	jsonReporter: {
		fileName: "reports/mutation/mutation.json",
	},
	tempDirName: ".stryker-tmp",
	cleanTempDir: true,
	// Thresholds calibrated from the measured 2026-10-24 baseline score of 82.89
	// (1027 killed+timeout of 1239 mutants; see MUTATION.md):
	//   break 80 = floor(82.89) - 2 safety margin
	//   low    82 = just below the measured score (regression signal)
	//   high   85 = improvement target (kills the blind spots listed in MUTATION.md)
	thresholds: {
		break: 80,
		high: 85,
		low: 82,
	},
};
