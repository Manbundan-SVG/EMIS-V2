import type { NextConfig } from 'next';

const nextConfig: NextConfig = {
  transpilePackages: ["@emis/signal-registry", "@emis/types"],
  experimental: {
    typedRoutes: true,
    // Windows + OneDrive can intermittently deny child-process worker spawns.
    // Keep the build in-process so local build/start stays reliable.
    webpackBuildWorker: false,
  },
};

export default nextConfig;
