import { task } from "@trigger.dev/sdk";

export const example = task({
  id: "example",
  run: async (payload: { message?: string }) => {
    const msg = payload.message ?? "Hello from Trigger.dev!";
    console.log(msg);
    return { ok: true, message: msg };
  },
});
