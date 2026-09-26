/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_URL: string;
  readonly VITE_DEV_USER_EMAIL: string;
  /** Local backend port (dev only); vite.config.ts fills it from KASAL_PORT. */
  readonly VITE_KASAL_PORT?: string;
  readonly DEV: boolean;
  readonly PROD: boolean;
  readonly MODE: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
