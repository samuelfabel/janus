import { defineConfig } from "vite";

// Project Pages URL: https://samuelfabel.github.io/janus/
export default defineConfig({
  base: "/janus/",
  root: ".",
  publicDir: "public",
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
