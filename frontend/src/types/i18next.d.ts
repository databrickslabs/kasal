import 'i18next';
import { CustomTypeOptions } from 'react-i18next';

declare module 'react-i18next' {
  interface CustomTypeOptions {
    defaultNS: 'translation';
    resources: {
      translation: {
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
      };
    };
  }
} 