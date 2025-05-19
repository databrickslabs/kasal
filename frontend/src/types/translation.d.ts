declare interface TranslationKeys {
  common: {
    save: string;
    cancel: string;
    loading: string;
    error: string;
    success: string;
  };
  navigation: {
    nemo: string;
    runs: string;
    logs: string;
    tools: string;
    ucTools: string;
    apiKeys: string;
    configuration: string;
  };
  configuration: {
    title: string;
    language: {
      title: string;
      select: string;
      saved: string;
    };
    databricks: {
      title: string;
      workspaceUrl: string;
      warehouseId: string;
      catalog: string;
      schema: string;
      secretScope: string;
      urlPlaceholder: string;
      saved: string;
      info: string;
    };
  };
}

declare module 'react-i18next' {
  interface TFunction {
    (key: string): string;
    (key: string, options: object): string;
  }

  export function useTranslation(): {
    t: TFunction;
    i18n: {
      changeLanguage: (lang: string) => Promise<void>;
      language: string;
    };
  };

  export const initReactI18next: {
    type: 'backend' | 'logger' | 'postProcessor' | 'formatter' | 'i18nFormat';
    init: (instance: any) => void;
  } & {
    [key: string]: any;
  };
} 