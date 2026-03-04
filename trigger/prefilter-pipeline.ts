import { task } from "@trigger.dev/sdk";
import { exec } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";

const execAsync = promisify(exec);

/**
 * Runs the Surfline pre_filter.py pipeline on Trigger.dev.
 * Pass input CSV path (relative to project root), output path, and optional config path.
 * Requires Python 3 with: requests, pyyaml, tqdm
 */
export const prefilterPipeline = task({
  id: "prefilter-pipeline",
  maxDuration: 600, // 10 min for large CSVs
  run: async (
    payload: {
      inputCsv: string;
      outputCsv?: string;
      configPath?: string;
      layer1Only?: boolean;
    },
    { ctx }
  ) => {
    const projectRoot = process.cwd();
    const input = resolve(projectRoot, payload.inputCsv);
    const output = resolve(
      projectRoot,
      payload.outputCsv ?? "filtered_output.csv"
    );
    const config = payload.configPath
      ? resolve(projectRoot, payload.configPath)
      : resolve(projectRoot, "config.yaml");
    const script = resolve(projectRoot, "pre_filter.py");

    const args = [
      script,
      "--input",
      input,
      "--output",
      output,
      ...(payload.layer1Only ? ["--layer1-only"] : ["--config", config]),
    ].join(" ");
    const cmd = `python3 ${args}`;
    const { stdout, stderr } = await execAsync(cmd, {
      cwd: projectRoot,
      maxBuffer: 10 * 1024 * 1024,
    });

    return {
      ok: true,
      input: payload.inputCsv,
      output: payload.outputCsv ?? "filtered_output.csv",
      stdout: stdout.slice(-2000),
      stderr: stderr.slice(-1000) || undefined,
    };
  },
});
