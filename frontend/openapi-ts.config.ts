import { defineConfig } from "@hey-api/openapi-ts";

export default defineConfig({
  input: "/tmp/phlower-openapi.json",
  output: { path: "src/api/generated" },
  plugins: [
    "@hey-api/typescript",
    "@hey-api/sdk",
    {
      name: "@tanstack/react-query",
      queryOptions: true,
    },
  ],
});
