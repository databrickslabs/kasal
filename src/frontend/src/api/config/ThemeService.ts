export interface ThemeConfig {
  theme: string;
}

export class ThemeService {
  private static instance: ThemeService;
  private readonly THEME_KEY = 'APP_THEME';

  // Private constructor to prevent direct instantiation
  private constructor() {
    // Singleton instance initialization
  }

  public static getInstance(): ThemeService {
    if (!ThemeService.instance) {
      ThemeService.instance = new ThemeService();
    }
    return ThemeService.instance;
  }

  public async getThemeConfig(): Promise<ThemeConfig> {
    const savedTheme = localStorage.getItem(this.THEME_KEY);
    if (savedTheme) {
      try {
        return JSON.parse(savedTheme);
      } catch (error) {
        console.error('Error parsing saved theme:', error);
      }
    }
    return { theme: 'professional' }; // Default theme
  }

  public async setThemeConfig(config: ThemeConfig): Promise<ThemeConfig> {
    localStorage.setItem(this.THEME_KEY, JSON.stringify(config));
    return config;
  }

  public async initializeTheme(): Promise<ThemeConfig> {
    return await this.getThemeConfig();
  }
} 